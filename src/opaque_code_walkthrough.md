# Opaque code walkthrough

## Code organization

Opaque code is under `src`

- Scala code under `src/main`
- C++ code is under `src/enclave`
- Flatbuffers is used for serializing data and expressions, and the definitions are under `src/flatbuffers`

Test data is located in `data`, which is automatically generated during build time.

## Opaque overview

### Interface

Opaque supports DataFrame, it's similar to a SQL table (or an intermediate representation). It defines a set of API calls (similar to SQL operators) that are supported in Spark SQL, but only a simpler subset is supported in Opaque. 

Opaque also supports the SQL interface, as seen here: [https://github.com/wzheng/opaque/blob/master/src/test/scala/edu/berkeley/cs/rise/opaque/OpaqueOperatorTests.scala#L486](https://github.com/wzheng/opaque/blob/master/src/test/scala/edu/berkeley/cs/rise/opaque/OpaqueOperatorTests.scala#L486)

### Initialization

Remote attestation

- `initRA()` called from the Scala side: [https://github.com/wzheng/opaque/blob/master/src/main/scala/edu/berkeley/cs/rise/opaque/RA.scala](https://github.com/wzheng/opaque/blob/master/src/main/scala/edu/berkeley/cs/rise/opaque/RA.scala)
- C++ ServiceProvider's code
[https://github.com/wzheng/opaque/blob/master/src/enclave/ServiceProvider/ServiceProvider.cpp#L237](https://github.com/wzheng/opaque/blob/master/src/enclave/ServiceProvider/ServiceProvider.cpp#L237)

Register hooks with Spark SQL

- Call `Utils.initSQLContext`
[https://github.com/mc2-project/opaque/blob/master/src/main/scala/edu/berkeley/cs/rise/opaque/Utils.scala](https://github.com/mc2-project/opaque/blob/master/src/main/scala/edu/berkeley/cs/rise/opaque/Utils.scala)

### Life of an Opaque query

Data loading

- Encrypt an existing DataFrame `df.encrypted`
[https://github.com/mc2-project/opaque/blob/master/src/main/scala/org/apache/spark/sql/OpaqueDatasetFunctions.scala](https://github.com/mc2-project/opaque/blob/master/src/main/scala/org/apache/spark/sql/OpaqueDatasetFunctions.scala), which creates an `Encrypt` logical plan node.
- Or load an encrypted file from disk

```scala
spark.read.format("edu.berkeley.cs.rise.opaque.EncryptedSource").schema(...).load("filename.encrypted")
```

- [https://github.com/mc2-project/opaque/blob/master/src/main/scala/edu/berkeley/cs/rise/opaque/sources.scala](https://github.com/mc2-project/opaque/blob/master/src/main/scala/edu/berkeley/cs/rise/opaque/sources.scala), which creates an `EncryptedScan` logical plan node.

Executing a query

- Apply operations to it like `.filter().select().join().groupBy().agg()`. These build up a logical plan.
- Execute an action like `.count()` or `.collect()` or `.show()`. This optimizes and executes the plan.
    - `ConvertToOpaqueOperators` rule determines which logical operators within the plan should be encrypted:
    [https://github.com/mc2-project/opaque/blob/master/src/main/scala/edu/berkeley/cs/rise/opaque/logical/rules.scala](https://github.com/mc2-project/opaque/blob/master/src/main/scala/edu/berkeley/cs/rise/opaque/logical/rules.scala)
    - `OpaqueOperators` strategy turns the logical plan into a physical plan:
    [https://github.com/mc2-project/opaque/blob/master/src/main/scala/edu/berkeley/cs/rise/opaque/strategies.scala](https://github.com/mc2-project/opaque/blob/master/src/main/scala/edu/berkeley/cs/rise/opaque/strategies.scala)
    - `executeCollect()` is called on the top-level plan node, which recursively calls `executeBlocked()` on its children to build an RDD that will execute the plan:
    [https://github.com/mc2-project/opaque/blob/master/src/main/scala/edu/berkeley/cs/rise/opaque/execution/operators.scala](https://github.com/mc2-project/opaque/blob/master/src/main/scala/edu/berkeley/cs/rise/opaque/execution/operators.scala)
    - The `executeBlocked()` overrides use Flatbuffers to encrypt and decrypt data and plan information on the driver:
    [https://github.com/mc2-project/opaque/blob/master/src/main/scala/edu/berkeley/cs/rise/opaque/Utils.scala#L654](https://github.com/mc2-project/opaque/blob/master/src/main/scala/edu/berkeley/cs/rise/opaque/Utils.scala#L654)
    - The generated RDD contains JNI calls to `SGXEnclave` to instruct the workers how to operate on encrypted data:
    [https://github.com/mc2-project/opaque/blob/master/src/main/scala/edu/berkeley/cs/rise/opaque/execution/SGXEnclave.scala](https://github.com/mc2-project/opaque/blob/master/src/main/scala/edu/berkeley/cs/rise/opaque/execution/SGXEnclave.scala)
    - These JNI calls in turn make ecalls to enter the enclave:
    [https://github.com/mc2-project/opaque/blob/master/src/enclave/App/App.cpp#L292](https://github.com/mc2-project/opaque/blob/master/src/enclave/App/App.cpp#L292)
    - Inside the enclave, we use Flatbuffers to decrypt the data and plan information and execute each operator:
    [https://github.com/mc2-project/opaque/blob/master/src/enclave/Enclave/Filter.cpp#L10](https://github.com/mc2-project/opaque/blob/master/src/enclave/Enclave/Filter.cpp#L10)
    - We have some abstractions to make it easier to work with data inside the enclave:
        - [https://github.com/mc2-project/opaque/blob/master/src/enclave/Enclave/Flatbuffers.h](https://github.com/mc2-project/opaque/blob/master/src/enclave/Enclave/Flatbuffers.h)
        - [https://github.com/mc2-project/opaque/blob/master/src/enclave/Enclave/FlatbuffersReaders.h](https://github.com/mc2-project/opaque/blob/master/src/enclave/Enclave/FlatbuffersReaders.h)
        - [https://github.com/mc2-project/opaque/blob/master/src/enclave/Enclave/ExpressionEvaluation.h](https://github.com/mc2-project/opaque/blob/master/src/enclave/Enclave/ExpressionEvaluation.h)
        - [https://github.com/mc2-project/opaque/blob/master/src/enclave/Enclave/FlatbuffersWriters.h](https://github.com/mc2-project/opaque/blob/master/src/enclave/Enclave/FlatbuffersWriters.h)