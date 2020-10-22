/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.cs.rise.opaque.benchmark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier

import edu.berkeley.cs.rise.opaque.Utils

object TPCDS {

  // Taken from https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/TPCDSBase.scala
  // Decimal modified to FLOAT

  private val tableColumns = Map(
    "store_sales" ->
      """
        |`ss_sold_date_sk` INT, `ss_sold_time_sk` INT, `ss_item_sk` INT, `ss_customer_sk` INT,
        |`ss_cdemo_sk` INT, `ss_hdemo_sk` INT, `ss_addr_sk` INT, `ss_store_sk` INT,
        |`ss_promo_sk` INT, `ss_ticket_number` INT, `ss_quantity` INT,
        |`ss_wholesale_cost` FLOAT, `ss_list_price` FLOAT,
        |`ss_sales_price` FLOAT, `ss_ext_discount_amt` FLOAT,
        |`ss_ext_sales_price` FLOAT, `ss_ext_wholesale_cost` FLOAT,
        |`ss_ext_list_price` FLOAT, `ss_ext_tax` FLOAT,
        |`ss_coupon_amt` FLOAT, `ss_net_paid` FLOAT,
        |`ss_net_paid_inc_tax` FLOAT, `ss_net_profit` FLOAT
      """.stripMargin,
    "store_returns" ->
      """
        |`sr_returned_date_sk` BIGINT, `sr_return_time_sk` BIGINT, `sr_item_sk` BIGINT,
        |`sr_customer_sk` BIGINT, `sr_cdemo_sk` BIGINT, `sr_hdemo_sk` BIGINT, `sr_addr_sk` BIGINT,
        |`sr_store_sk` BIGINT, `sr_reason_sk` BIGINT, `sr_ticket_number` BIGINT,
        |`sr_return_quantity` INT, `sr_return_amt` FLOAT,
        |`sr_return_amt_inc_tax` FLOAT,
        |`sr_return_ship_cost` FLOAT,
        |`sr_reversed_charge` FLOAT,
        |`sr_net_loss` FLOAT
      """.stripMargin,
    "catalog_sales" ->
      """
        |`cs_sold_date_sk` INT, `cs_sold_time_sk` INT, `cs_ship_date_sk` INT,
        |`cs_bill_customer_sk` INT, `cs_bill_cdemo_sk` INT, `cs_bill_hdemo_sk` INT,
        |`cs_bill_addr_sk` INT, `cs_ship_customer_sk` INT, `cs_ship_cdemo_sk` INT,
        |`cs_ship_hdemo_sk` INT, `cs_ship_addr_sk` INT, `cs_call_center_sk` INT,
        |`cs_catalog_page_sk` INT, `cs_ship_mode_sk` INT, `cs_warehouse_sk` INT,
        |`cs_item_sk` INT, `cs_promo_sk` INT, `cs_order_number` INT, `cs_quantity` INT,
        |`cs_wholesale_cost` FLOAT,
        |`cs_sales_price` FLOAT,
        |`cs_ext_sales_price` FLOAT,
        |`cs_ext_list_price` FLOAT,
        |`cs_coupon_amt` FLOAT,
        |`cs_net_paid` FLOAT,
        |`cs_net_paid_inc_ship` FLOAT,
        |`cs_net_profit` FLOAT
      """.stripMargin,
    "catalog_returns" ->
      """
        |`cr_returned_date_sk` INT, `cr_returned_time_sk` INT, `cr_item_sk` INT,
        |`cr_refunded_customer_sk` INT, `cr_refunded_cdemo_sk` INT, `cr_refunded_hdemo_sk` INT,
        |`cr_refunded_addr_sk` INT, `cr_returning_customer_sk` INT, `cr_returning_cdemo_sk` INT,
        |`cr_returning_hdemo_sk` INT, `cr_returning_addr_sk` INT, `cr_call_center_sk` INT,
        |`cr_catalog_page_sk` INT, `cr_ship_mode_sk` INT, `cr_warehouse_sk` INT,
        |`cr_reason_sk` INT,`cr_order_number` INT, `cr_return_quantity` INT,
        |`cr_return_amount` FLOAT,
        |`cr_return_amt_inc_tax` FLOAT,
        |`cr_return_ship_cost` FLOAT,
        |`cr_reversed_charge` FLOAT,
        |`cr_net_loss` FLOAT
      """.stripMargin,
    "web_sales" ->
      """
        |`ws_sold_date_sk` INT, `ws_sold_time_sk` INT, `ws_ship_date_sk` INT, `ws_item_sk` INT,
        |`ws_bill_customer_sk` INT, `ws_bill_cdemo_sk` INT, `ws_bill_hdemo_sk` INT,
        |`ws_bill_addr_sk` INT, `ws_ship_customer_sk` INT, `ws_ship_cdemo_sk` INT,
        |`ws_ship_hdemo_sk` INT, `ws_ship_addr_sk` INT, `ws_web_page_sk` INT,
        |`ws_web_site_sk` INT, `ws_ship_mode_sk` INT, `ws_warehouse_sk` INT, `ws_promo_sk` INT,
        |`ws_order_number` INT, `ws_quantity` INT, `ws_wholesale_cost` FLOAT,
        |`ws_list_price` FLOAT,
        |`ws_ext_discount_amt` FLOAT,
        |`ws_ext_wholesale_cost` FLOAT,
        |`ws_ext_tax` FLOAT,
        |`ws_net_paid` FLOAT,
        |`ws_net_paid_inc_ship` FLOAT,
        |`ws_net_profit` FLOAT
      """.stripMargin,
    "web_returns" ->
      """
        |`wr_returned_date_sk` BIGINT, `wr_returned_time_sk` BIGINT, `wr_item_sk` BIGINT,
        |`wr_refunded_customer_sk` BIGINT, `wr_refunded_cdemo_sk` BIGINT,
        |`wr_refunded_hdemo_sk` BIGINT, `wr_refunded_addr_sk` BIGINT,
        |`wr_returning_customer_sk` BIGINT, `wr_returning_cdemo_sk` BIGINT,
        |`wr_returning_hdemo_sk` BIGINT, `wr_returning_addr_sk` BIGINT, `wr_web_page_sk` BIGINT,
        |`wr_reason_sk` BIGINT, `wr_order_number` BIGINT, `wr_return_quantity` INT,
        |`wr_return_amt` FLOAT,
        |`wr_return_amt_inc_tax` FLOAT,
        |`wr_return_ship_cost` FLOAT,
        |`wr_reversed_charge` FLOAT,
        |`wr_net_loss` FLOAT
      """.stripMargin,
    "inventory" ->
      """
        |`inv_date_sk` INT, `inv_item_sk` INT, `inv_warehouse_sk` INT,
        |`inv_quantity_on_hand` INT
      """.stripMargin,
    "store" ->
      """
        |`s_store_sk` INT, `s_store_id` STRING, `s_rec_start_date` DATE,
        |`s_rec_end_date` DATE, `s_closed_date_sk` INT, `s_store_name` STRING,
        |`s_number_employees` INT, `s_floor_space` INT, `s_hours` STRING, `s_manager` STRING,
        |`s_market_id` INT, `s_geography_class` STRING, `s_market_desc` STRING,
        |`s_market_manager` STRING, `s_division_id` INT, `s_division_name` STRING,
        |`s_company_id` INT, `s_company_name` STRING, `s_street_number` STRING,
        |`s_street_name` STRING, `s_street_type` STRING, `s_suite_number` STRING, `s_city` STRING,
        |`s_county` STRING, `s_state` STRING, `s_zip` STRING, `s_country` STRING,
        |`s_gmt_offset` FLOAT
      """.stripMargin,
    "call_center" ->
      """
        |`cc_call_center_sk` INT, `cc_call_center_id` STRING, `cc_rec_start_date` DATE,
        |`cc_rec_end_date` DATE, `cc_closed_date_sk` INT, `cc_open_date_sk` INT, `cc_name` STRING,
        |`cc_class` STRING, `cc_employees` INT, `cc_sq_ft` INT, `cc_hours` STRING,
        |`cc_manager` STRING, `cc_mkt_id` INT, `cc_mkt_class` STRING, `cc_mkt_desc` STRING,
        |`cc_market_manager` STRING, `cc_division` INT, `cc_division_name` STRING,
        |`cc_company` INT, `cc_company_name` STRING, `cc_street_number` STRING,
        |`cc_street_name` STRING, `cc_street_type` STRING, `cc_suite_number` STRING,
        |`cc_city` STRING, `cc_county` STRING, `cc_state` STRING, `cc_zip` STRING,
        |`cc_country` STRING, `cc_gmt_offset` FLOAT
      """.stripMargin,
    "catalog_page" ->
      """
        |`cp_catalog_page_sk` INT, `cp_catalog_page_id` STRING, `cp_start_date_sk` INT,
        |`cp_end_date_sk` INT, `cp_department` STRING, `cp_catalog_number` INT,
        |`cp_catalog_page_number` INT, `cp_description` STRING, `cp_type` STRING
      """.stripMargin,
    "web_site" ->
      """
        |`web_site_sk` INT, `web_site_id` STRING, `web_rec_start_date` DATE,
        |`web_rec_end_date` DATE, `web_name` STRING, `web_open_date_sk` INT,
        |`web_close_date_sk` INT, `web_class` STRING, `web_manager` STRING, `web_mkt_id` INT,
        |`web_mkt_class` STRING, `web_mkt_desc` STRING, `web_market_manager` STRING,
        |`web_company_id` INT, `web_company_name` STRING, `web_street_number` STRING,
        |`web_street_name` STRING, `web_street_type` STRING, `web_suite_number` STRING,
        |`web_city` STRING, `web_county` STRING, `web_state` STRING, `web_zip` STRING,
        |`web_country` STRING, `web_gmt_offset` FLOAT
      """.stripMargin,
    "web_page" ->
      """
        |`wp_web_page_sk` INT, `wp_web_page_id` STRING,
        |`wp_rec_start_date` DATE, `wp_rec_end_date` DATE, `wp_creation_date_sk` INT,
        |`wp_access_date_sk` INT, `wp_autogen_flag` STRING, `wp_customer_sk` INT,
        |`wp_url` STRING, `wp_type` STRING, `wp_char_count` INT, `wp_link_count` INT,
        |`wp_image_count` INT, `wp_max_ad_count` INT
      """.stripMargin,
    "warehouse" ->
      """
        |`w_warehouse_sk` INT, `w_warehouse_id` STRING, `w_warehouse_name` STRING,
        |`w_warehouse_sq_ft` INT, `w_street_number` STRING, `w_street_name` STRING,
        |`w_street_type` STRING, `w_suite_number` STRING, `w_city` STRING, `w_county` STRING,
        |`w_state` STRING, `w_zip` STRING, `w_country` STRING, `w_gmt_offset` FLOAT
      """.stripMargin,
    "customer" ->
      """
        |`c_customer_sk` INT, `c_customer_id` STRING, `c_current_cdemo_sk` INT,
        |`c_current_hdemo_sk` INT, `c_current_addr_sk` INT, `c_first_shipto_date_sk` INT,
        |`c_first_sales_date_sk` INT, `c_salutation` STRING, `c_first_name` STRING,
        |`c_last_name` STRING, `c_preferred_cust_flag` STRING, `c_birth_day` INT,
        |`c_birth_month` INT, `c_birth_year` INT, `c_birth_country` STRING, `c_login` STRING,
        |`c_email_address` STRING, `c_last_review_date` INT
      """.stripMargin,
    "customer_address" ->
      """
        |`ca_address_sk` INT, `ca_address_id` STRING, `ca_street_number` STRING,
        |`ca_street_name` STRING, `ca_street_type` STRING, `ca_suite_number` STRING,
        |`ca_city` STRING, `ca_county` STRING, `ca_state` STRING, `ca_zip` STRING,
        |`ca_country` STRING, `ca_gmt_offset` FLOAT, `ca_location_type` STRING
      """.stripMargin,
    "customer_demographics" ->
      """
        |`cd_demo_sk` INT, `cd_gender` STRING, `cd_marital_status` STRING,
        |`cd_education_status` STRING, `cd_purchase_estimate` INT, `cd_credit_rating` STRING,
        |`cd_dep_count` INT, `cd_dep_employed_count` INT, `cd_dep_college_count` INT
      """.stripMargin,
    "date_dim" ->
      """
        |`d_date_sk` INT, `d_date_id` STRING, `d_date` DATE,
        |`d_month_seq` INT, `d_week_seq` INT, `d_quarter_seq` INT, `d_year` INT, `d_dow` INT,
        |`d_moy` INT, `d_dom` INT, `d_qoy` INT, `d_fy_year` INT, `d_fy_quarter_seq` INT,
        |`d_fy_week_seq` INT, `d_day_name` STRING, `d_quarter_name` STRING, `d_holiday` STRING,
        |`d_weekend` STRING, `d_following_holiday` STRING, `d_first_dom` INT, `d_last_dom` INT,
        |`d_same_day_ly` INT, `d_same_day_lq` INT, `d_current_day` STRING,
        |`d_current_week` STRING, `d_current_month` STRING, `d_current_quarter` STRING,
        |`d_current_year` STRING
      """.stripMargin,
    "household_demographics" ->
      """
        |`hd_demo_sk` INT, `hd_income_band_sk` INT, `hd_buy_potential` STRING, `hd_dep_count` INT,
        |`hd_vehicle_count` INT
      """.stripMargin,
    "item" ->
      """
        |`i_item_sk` INT, `i_item_id` STRING, `i_rec_start_date` DATE,
        |`i_rec_end_date` DATE, `i_item_desc` STRING, `i_current_price` FLOAT,
        |`i_wholesale_cost` FLOAT, `i_brand_id` INT, `i_brand` STRING, `i_class_id` INT,
        |`i_class` STRING, `i_category_id` INT, `i_category` STRING, `i_manufact_id` INT,
        |`i_manufact` STRING, `i_size` STRING, `i_formulation` STRING, `i_color` STRING,
        |`i_units` STRING, `i_container` STRING, `i_manager_id` INT, `i_product_name` STRING
      """.stripMargin,
    "income_band" ->
      """
        |`ib_income_band_sk` INT, `ib_lower_bound` INT, `ib_upper_bound` INT
      """.stripMargin,
    "promotion" ->
      """
        |`p_promo_sk` INT, `p_promo_id` STRING, `p_start_date_sk` INT, `p_end_date_sk` INT,
        |`p_item_sk` INT, `p_cost` FLOAT,
        |`p_channel_dmail` STRING, `p_channel_email` STRING, `p_channel_catalog` STRING,
        |`p_channel_tv` STRING, `p_channel_radio` STRING, `p_channel_press` STRING,
        |`p_channel_event` STRING, `p_channel_demo` STRING, `p_channel_details` STRING,
        |`p_purpose` STRING, `p_discount_active` STRING
      """.stripMargin,
    "reason" ->
      """
        |`r_reason_sk` INT, `r_reason_id` STRING, `r_reason_desc` STRING
      """.stripMargin,
    "ship_mode" ->
      """
        |`sm_ship_mode_sk` INT, `sm_ship_mode_id` STRING, `sm_type` STRING, `sm_code` STRING,
        |`sm_carrier` STRING, `sm_contract` STRING
      """.stripMargin,
    "time_dim" ->
      """
        |`t_time_sk` INT, `t_time_id` STRING, `t_time` INT, `t_hour` INT, `t_minute` INT,
        |`t_second` INT, `t_am_pm` STRING, `t_shift` STRING, `t_sub_shift` STRING,
        |`t_meal_time` STRING
      """.stripMargin
  )

  val tableNames: Iterable[String] = tableColumns.keys

  def createTable(
    spark: SparkSession,
    tableName: String,
    format: String = "parquet",
    options: Seq[String] = Nil): Unit = {
    spark.sql(
      s"""
         |CREATE TABLE `$tableName` (${tableColumns(tableName)})
         |USING $format
         |${options.mkString("\n")}
       """.stripMargin)
  }

  private def init(sqlContext: SQLContext, numPartitions: Int, securityLevel: SecurityLevel, loadTables: Seq[String]) = {
    import edu.berkeley.cs.rise.opaque.implicits._
    val spark = sqlContext.sparkSession

    val path = s"${Benchmark.dataDir}/tpcds/data/"
    for (tableName <- loadTables) {
      createTable(spark, tableName, format="csv", options=Seq(s"""OPTIONS (path "${path}${tableName}.dat")"""))
    }

    securityLevel match {
      case Encrypted => {
        for (tableName <- loadTables) {
          val df = spark.sql(s"""SELECT * FROM ${tableName}""".stripMargin).repartition(numPartitions).encrypted
          df.createOrReplaceTempView(s"""${tableName}_enc""")
          df.cache()
        }
      }

      case Insecure => {}
    }
  }

  private def clearTables(spark: SparkSession, securityLevel: SecurityLevel, loadTables: Seq[String]) = {
    for (tableName <- loadTables) {
      spark.sql(s"""DROP TABLE IF EXISTS default.${tableName}""".stripMargin)
    }

    securityLevel match {
      case Encrypted => {
        for (tableName <- loadTables) {
          spark.catalog.dropGlobalTempView(s"${tableName}_enc")
        }
      }
      case Insecure => {}
    }
  }

  // SQL queries taken from https://github.com/apache/spark/tree/master/sql/core/src/test/resources/tpcds
  def tpcdsQuery(queryNumber: Int, securityLevel: SecurityLevel) : String = {
    val isEnc = securityLevel match {
      case Insecure => ""
      case Encrypted => "_enc"
    }

    val queryStr = queryNumber match {
      case 1 => {
        s"""|WITH customer_total_return AS
            |( SELECT
            |    sr_customer_sk AS ctr_customer_sk,
            |    sr_store_sk AS ctr_store_sk,
            |    sum(sr_return_amt) AS ctr_total_return
            |  FROM store_returns${isEnc} ss, date_dim${isEnc} dt
            |  WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
            |  GROUP BY sr_customer_sk, sr_store_sk)
            |SELECT c_customer_id
            |FROM customer_total_return ctr1, store${isEnc} s, customer${isEnc} c
            |WHERE ctr1.ctr_total_return >
            |  (SELECT avg(ctr_total_return) * 1.2
            |  FROM customer_total_return ctr2
            |  WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
            |  AND s_store_sk = ctr1.ctr_store_sk
            |  AND s_state = 'TN'
            |  AND ctr1.ctr_customer_sk = c_customer_sk
            |ORDER BY c_customer_id
            |LIMIT 100"""
      }

      case 3 => {
        s"""|SELECT
            |  dt.d_year,
            |  it.i_brand_id brand_id,
            |  it.i_brand brand,
            |  SUM(ss_ext_sales_price) sum_agg
            |FROM date_dim${isEnc} dt, store_sales${isEnc} ss, item${isEnc} it
            |WHERE dt.d_date_sk = ss.ss_sold_date_sk
            |  AND ss.ss_item_sk = it.i_item_sk
            |  AND it.i_manufact_id = 128
            |  AND dt.d_moy = 11
            |GROUP BY dt.d_year, it.i_brand, it.i_brand_id
            |ORDER BY dt.d_year, sum_agg DESC, brand_id
            |LIMIT 100 """
      }

      case 7 => {
        s"""|SELECT
            |  i_item_id,
            |  avg(ss_quantity) agg1,
            |  avg(ss_list_price) agg2,
            |  avg(ss_coupon_amt) agg3,
            |  avg(ss_sales_price) agg4
            |FROM store_sales${isEnc}, customer_demographics${isEnc}, date_dim${isEnc}, item${isEnc}, promotion${isEnc}
            |WHERE ss_sold_date_sk = d_date_sk AND
            |  ss_item_sk = i_item_sk AND
            |  ss_cdemo_sk = cd_demo_sk AND
            |  ss_promo_sk = p_promo_sk AND
            |  cd_gender = 'M' AND
            |  cd_marital_status = 'S' AND
            |  cd_education_status = 'College' AND
            |  (p_channel_email = 'N' OR p_channel_event = 'N') AND
            |  d_year = 2000
            |GROUP BY i_item_id
            |ORDER BY i_item_id
            |LIMIT 100"""
      }

      case 19 => {
        s"""|SELECT
            |  i_brand_id brand_id,
            |  i_brand brand,
            |  i_manufact_id,
            |  i_manufact,
            |  sum(ss_ext_sales_price) ext_price
            |FROM date_dim${isEnc}, store_sales${isEnc}, item${isEnc}, customer${isEnc}, customer_address${isEnc}, store${isEnc}
            |WHERE d_date_sk = ss_sold_date_sk
            |  AND ss_item_sk = i_item_sk
            |  AND i_manager_id = 8
            |  AND d_moy = 11
            |  AND d_year = 1998
            |  AND ss_customer_sk = c_customer_sk
            |  AND c_current_addr_sk = ca_address_sk
            |  AND substr(ca_zip, 1, 5) <> substr(s_zip, 1, 5)
            |  AND ss_store_sk = s_store_sk
            |GROUP BY i_brand, i_brand_id, i_manufact_id, i_manufact
            |ORDER BY ext_price DESC, brand, brand_id, i_manufact_id, i_manufact
            |LIMIT 100"""
      }

      case 26 => {
        s"""|SELECT
            |  i_item_id,
            |  avg(cs_quantity) agg1,
            |  avg(cs_list_price) agg2,
            |  avg(cs_coupon_amt) agg3,
            |  avg(cs_sales_price) agg4
            |FROM catalog_sales${isEnc}, customer_demographics${isEnc}, date_dim${isEnc}, item${isEnc}, promotion${isEnc}
            |WHERE cs_sold_date_sk = d_date_sk AND
            |  cs_item_sk = i_item_sk AND
            |  cs_bill_cdemo_sk = cd_demo_sk AND
            |  cs_promo_sk = p_promo_sk AND
            |  cd_gender = 'M' AND
            |  cd_marital_status = 'S' AND
            |  cd_education_status = 'College' AND
            |  (p_channel_email = 'N' OR p_channel_event = 'N') AND
            |  d_year = 2000
            |GROUP BY i_item_id
            |ORDER BY i_item_id
            |LIMIT 100"""
      }

      case 28 => {
        s"""|SELECT *
            |FROM (SELECT
            |  avg(ss_list_price) B1_LP,
            |  count(ss_list_price) B1_CNT,
            |  count(DISTINCT ss_list_price) B1_CNTD
            |FROM store_sales${isEnc}
            |WHERE ss_quantity BETWEEN 0 AND 5
            |  AND (ss_list_price BETWEEN 8 AND 8 + 10
            |  OR ss_coupon_amt BETWEEN 459 AND 459 + 1000
            |  OR ss_wholesale_cost BETWEEN 57 AND 57 + 20)) B1,
            |  (SELECT
            |    avg(ss_list_price) B2_LP,
            |    count(ss_list_price) B2_CNT,
            |    count(DISTINCT ss_list_price) B2_CNTD
            |  FROM store_sales${isEnc}
            |  WHERE ss_quantity BETWEEN 6 AND 10
            |    AND (ss_list_price BETWEEN 90 AND 90 + 10
            |    OR ss_coupon_amt BETWEEN 2323 AND 2323 + 1000
            |    OR ss_wholesale_cost BETWEEN 31 AND 31 + 20)) B2,
            |  (SELECT
            |    avg(ss_list_price) B3_LP,
            |    count(ss_list_price) B3_CNT,
            |    count(DISTINCT ss_list_price) B3_CNTD
            |  FROM store_sales${isEnc}
            |  WHERE ss_quantity BETWEEN 11 AND 15
            |    AND (ss_list_price BETWEEN 142 AND 142 + 10
            |    OR ss_coupon_amt BETWEEN 12214 AND 12214 + 1000
            |    OR ss_wholesale_cost BETWEEN 79 AND 79 + 20)) B3,
            |  (SELECT
            |    avg(ss_list_price) B4_LP,
            |    count(ss_list_price) B4_CNT,
            |    count(DISTINCT ss_list_price) B4_CNTD
            |  FROM store_sales${isEnc}
            |  WHERE ss_quantity BETWEEN 16 AND 20
            |    AND (ss_list_price BETWEEN 135 AND 135 + 10
            |    OR ss_coupon_amt BETWEEN 6071 AND 6071 + 1000
            |    OR ss_wholesale_cost BETWEEN 38 AND 38 + 20)) B4,
            |  (SELECT
            |    avg(ss_list_price) B5_LP,
            |    count(ss_list_price) B5_CNT,
            |    count(DISTINCT ss_list_price) B5_CNTD
            |  FROM store_sales${isEnc}
            |  WHERE ss_quantity BETWEEN 21 AND 25
            |    AND (ss_list_price BETWEEN 122 AND 122 + 10
            |    OR ss_coupon_amt BETWEEN 836 AND 836 + 1000
            |    OR ss_wholesale_cost BETWEEN 17 AND 17 + 20)) B5,
            |  (SELECT
            |    avg(ss_list_price) B6_LP,
            |    count(ss_list_price) B6_CNT,
            |    count(DISTINCT ss_list_price) B6_CNTD
            |  FROM store_sales${isEnc}
            |  WHERE ss_quantity BETWEEN 26 AND 30
            |    AND (ss_list_price BETWEEN 154 AND 154 + 10
            |    OR ss_coupon_amt BETWEEN 7326 AND 7326 + 1000
            |    OR ss_wholesale_cost BETWEEN 7 AND 7 + 20)) B6
            |LIMIT 100"""
      }

      case 81 => {
        s"""|WITH customer_total_return AS
            |(SELECT
            |    cr_returning_customer_sk AS ctr_customer_sk,
            |    ca_state AS ctr_state,
            |    sum(cr_return_amt_inc_tax) AS ctr_total_return
            |  FROM catalog_returns${isEnc}, date_dim${isEnc}, customer_address${isEnc}
            |  WHERE cr_returned_date_sk = d_date_sk
            |    AND d_year = 2000
            |    AND cr_returning_addr_sk = ca_address_sk
            |  GROUP BY cr_returning_customer_sk, ca_state )
            |SELECT
            |  c_customer_id,
            |  c_salutation,
            |  c_first_name,
            |  c_last_name,
            |  ca_street_number,
            |  ca_street_name,
            |  ca_street_type,
            |  ca_suite_number,
            |  ca_city,
            |  ca_county,
            |  ca_state,
            |  ca_zip,
            |  ca_country,
            |  ca_gmt_offset,
            |  ca_location_type,
            |  ctr_total_return
            |FROM customer_total_return ctr1, customer_address, customer
            |WHERE ctr1.ctr_total_return > (SELECT avg(ctr_total_return) * 1.2
            |FROM customer_total_return ctr2
            |WHERE ctr1.ctr_state = ctr2.ctr_state)
            |  AND ca_address_sk = c_current_addr_sk
            |  AND ca_state = 'GA'
            |  AND ctr1.ctr_customer_sk = c_customer_sk
            |ORDER BY c_customer_id, c_salutation, c_first_name, c_last_name, ca_street_number, ca_street_name
            |  , ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset
            |  , ca_location_type, ctr_total_return
            |LIMIT 100"""
      }

      case 88 => {
        s"""|SELECT *
            |FROM
            |  (SELECT count(*) h8_30_to_9
            |  FROM store_sales${isEnc}, household_demographics${isEnc}, time_dim${isEnc}, store${isEnc}
            |  WHERE ss_sold_time_sk = time_dim${isEnc}.t_time_sk
            |    AND ss_hdemo_sk = household_demographics${isEnc}.hd_demo_sk
            |    AND ss_store_sk = s_store_sk
            |    AND time_dim${isEnc}.t_hour = 8
            |    AND time_dim${isEnc}.t_minute >= 30
            |    AND (
            |    (household_demographics${isEnc}.hd_dep_count = 4 AND household_demographics${isEnc}.hd_vehicle_count <= 4 + 2)
            |      OR
            |      (household_demographics${isEnc}.hd_dep_count = 2 AND household_demographics${isEnc}.hd_vehicle_count <= 2 + 2)
            |      OR
            |      (household_demographics${isEnc}.hd_dep_count = 0 AND
            |        household_demographics${isEnc}.hd_vehicle_count <= 0 + 2))
            |    AND store${isEnc}.s_store_name = 'ese') s1,
            |  (SELECT count(*) h9_to_9_30
            |  FROM store_sales${isEnc}, household_demographics${isEnc}, time_dim${isEnc}, store${isEnc}
            |  WHERE ss_sold_time_sk = time_dim${isEnc}.t_time_sk
            |    AND ss_hdemo_sk = household_demographics${isEnc}.hd_demo_sk
            |    AND ss_store_sk = s_store_sk
            |    AND time_dim${isEnc}.t_hour = 9
            |    AND time_dim${isEnc}.t_minute < 30
            |    AND (
            |    (household_demographics${isEnc}.hd_dep_count = 4 AND household_demographics${isEnc}.hd_vehicle_count <= 4 + 2)
            |      OR
            |      (household_demographics${isEnc}.hd_dep_count = 2 AND household_demographics${isEnc}.hd_vehicle_count <= 2 + 2)
            |      OR
            |      (household_demographics${isEnc}.hd_dep_count = 0 AND
            |        household_demographics${isEnc}.hd_vehicle_count <= 0 + 2))
            |    AND store${isEnc}.s_store_name = 'ese') s2,
            |  (SELECT count(*) h9_30_to_10
            |  FROM store_sales${isEnc}, household_demographics${isEnc}, time_dim${isEnc}, store${isEnc}
            |  WHERE ss_sold_time_sk = time_dim${isEnc}.t_time_sk
            |    AND ss_hdemo_sk = household_demographics${isEnc}.hd_demo_sk
            |    AND ss_store_sk = s_store_sk
            |    AND time_dim${isEnc}.t_hour = 9
            |    AND time_dim${isEnc}.t_minute >= 30
            |    AND (
            |    (household_demographics${isEnc}.hd_dep_count = 4 AND household_demographics${isEnc}.hd_vehicle_count <= 4 + 2)
            |      OR
            |      (household_demographics${isEnc}.hd_dep_count = 2 AND household_demographics${isEnc}.hd_vehicle_count <= 2 + 2)
            |      OR
            |      (household_demographics${isEnc}.hd_dep_count = 0 AND
            |        household_demographics${isEnc}.hd_vehicle_count <= 0 + 2))
            |    AND store${isEnc}.s_store_name = 'ese') s3,
            |  (SELECT count(*) h10_to_10_30
            |  FROM store_sales${isEnc}, household_demographics${isEnc}, time_dim${isEnc}, store${isEnc}
            |  WHERE ss_sold_time_sk = time_dim${isEnc}.t_time_sk
            |    AND ss_hdemo_sk = household_demographics${isEnc}.hd_demo_sk
            |    AND ss_store_sk = s_store_sk
            |    AND time_dim${isEnc}.t_hour = 10
            |    AND time_dim${isEnc}.t_minute < 30
            |    AND (
            |    (household_demographics${isEnc}.hd_dep_count = 4 AND household_demographics${isEnc}.hd_vehicle_count <= 4 + 2)
            |      OR
            |      (household_demographics${isEnc}.hd_dep_count = 2 AND household_demographics${isEnc}.hd_vehicle_count <= 2 + 2)
            |      OR
            |      (household_demographics${isEnc}.hd_dep_count = 0 AND
            |        household_demographics${isEnc}.hd_vehicle_count <= 0 + 2))
            |    AND store${isEnc}.s_store_name = 'ese') s4,
            |  (SELECT count(*) h10_30_to_11
            |  FROM store_sales${isEnc}, household_demographics${isEnc}, time_dim${isEnc}, store${isEnc}
            |  WHERE ss_sold_time_sk = time_dim${isEnc}.t_time_sk
            |    AND ss_hdemo_sk = household_demographics${isEnc}.hd_demo_sk
            |    AND ss_store_sk = s_store_sk
            |    AND time_dim${isEnc}.t_hour = 10
            |    AND time_dim${isEnc}.t_minute >= 30
            |    AND (
            |    (household_demographics${isEnc}.hd_dep_count = 4 AND household_demographics${isEnc}.hd_vehicle_count <= 4 + 2)
            |      OR
            |      (household_demographics${isEnc}.hd_dep_count = 2 AND household_demographics${isEnc}.hd_vehicle_count <= 2 + 2)
            |      OR
            |      (household_demographics${isEnc}.hd_dep_count = 0 AND
            |        household_demographics${isEnc}.hd_vehicle_count <= 0 + 2))
            |    AND store${isEnc}.s_store_name = 'ese') s5,
            |  (SELECT count(*) h11_to_11_30
            |  FROM store_sales${isEnc}, household_demographics${isEnc}, time_dim${isEnc}, store${isEnc}
            |  WHERE ss_sold_time_sk = time_dim${isEnc}.t_time_sk
            |    AND ss_hdemo_sk = household_demographics${isEnc}.hd_demo_sk
            |    AND ss_store_sk = s_store_sk
            |    AND time_dim${isEnc}.t_hour = 11
            |    AND time_dim${isEnc}.t_minute < 30
            |    AND (
            |    (household_demographics${isEnc}.hd_dep_count = 4 AND household_demographics${isEnc}.hd_vehicle_count <= 4 + 2)
            |      OR
            |      (household_demographics${isEnc}.hd_dep_count = 2 AND household_demographics${isEnc}.hd_vehicle_count <= 2 + 2)
            |      OR
            |      (household_demographics${isEnc}.hd_dep_count = 0 AND
            |        household_demographics${isEnc}.hd_vehicle_count <= 0 + 2))
            |    AND store${isEnc}.s_store_name = 'ese') s6,
            |  (SELECT count(*) h11_30_to_12
            |  FROM store_sales${isEnc}, household_demographics${isEnc}, time_dim${isEnc}, store${isEnc}
            |  WHERE ss_sold_time_sk = time_dim${isEnc}.t_time_sk
            |    AND ss_hdemo_sk = household_demographics${isEnc}.hd_demo_sk
            |    AND ss_store_sk = s_store_sk
            |    AND time_dim${isEnc}.t_hour = 11
            |    AND time_dim${isEnc}.t_minute >= 30
            |    AND (
            |    (household_demographics${isEnc}.hd_dep_count = 4 AND household_demographics${isEnc}.hd_vehicle_count <= 4 + 2)
            |      OR
            |      (household_demographics${isEnc}.hd_dep_count = 2 AND household_demographics${isEnc}.hd_vehicle_count <= 2 + 2)
            |      OR
            |      (household_demographics${isEnc}.hd_dep_count = 0 AND
            |        household_demographics${isEnc}.hd_vehicle_count <= 0 + 2))
            |    AND store${isEnc}.s_store_name = 'ese') s7,
            |  (SELECT count(*) h12_to_12_30
            |  FROM store_sales${isEnc}, household_demographics${isEnc}, time_dim${isEnc}, store${isEnc}
            |  WHERE ss_sold_time_sk = time_dim${isEnc}.t_time_sk
            |    AND ss_hdemo_sk = household_demographics${isEnc}.hd_demo_sk
            |    AND ss_store_sk = s_store_sk
            |    AND time_dim${isEnc}.t_hour = 12
            |    AND time_dim${isEnc}.t_minute < 30
            |    AND (
            |    (household_demographics${isEnc}.hd_dep_count = 4 AND household_demographics${isEnc}.hd_vehicle_count <= 4 + 2)
            |      OR
            |      (household_demographics${isEnc}.hd_dep_count = 2 AND household_demographics${isEnc}.hd_vehicle_count <= 2 + 2)
            |      OR
            |      (household_demographics${isEnc}.hd_dep_count = 0 AND
            |        household_demographics${isEnc}.hd_vehicle_count <= 0 + 2))
            |    AND store${isEnc}.s_store_name = 'ese') s8"""
      }

      case 96 => {
        s"""|SELECT count(*)
            |FROM store_sales${isEnc}, household_demographics${isEnc}, time_dim${isEnc}, store${isEnc}
            |WHERE ss_sold_time_sk = time_dim${isEnc}.t_time_sk
            |  AND ss_hdemo_sk = household_demographics${isEnc}.hd_demo_sk
            |  AND ss_store_sk = s_store_sk
            |  AND time_dim${isEnc}.t_hour = 20
            |  AND time_dim${isEnc}.t_minute >= 30
            |  AND household_demographics${isEnc}.hd_dep_count = 7
            |  AND store${isEnc}.s_store_name = 'ese'
            |ORDER BY count(*)
            |LIMIT 100"""
      }
    }

    queryStr.stripMargin
  }

  /** List of Queries taken from https://github.com/apache/spark/tree/master/sql/core/src/test/resources/tpcds **/
  def tpcds(
    queryNumber: Int,
    sqlContext: SQLContext,
    securityLevel: SecurityLevel,
    numPartitions: Int) : Seq[Any] = {
    import sqlContext.implicits._

    val loadTables = queryNumber match {
      case 1 => Seq("store_returns", "date_dim", "store", "customer")
      case 3 => Seq("date_dim", "store_sales", "item")
      case 7 => Seq("store_sales", "customer_demographics", "date_dim", "item", "promotion")
      case 19 => Seq("date_dim", "store_sales", "item", "customer", "customer_address", "store")
      case 26 => Seq("catalog_sales", "customer_demographics", "date_dim", "item", "promotion")
      case 28 => Seq("store_sales")
      case 81 => Seq("catalog_returns", "date_dim", "customer_address", "customer")
      case 88 => Seq("store_sales", "household_demographics", "time_dim", "store")
      case 96 => Seq("store_sales", "household_demographics", "time_dim", "store")
      case _ => Seq("")
    }

    val secType = securityLevel match {
      case Insecure => "Spark"
      case Encrypted => "Encrypted"
    }

    init(sqlContext, numPartitions, securityLevel, loadTables)
    val sqlStr = tpcdsQuery(queryNumber, securityLevel)

    val result = Utils.time(s"""${secType}""") {
      sqlContext.sparkSession.sql(sqlStr).collect
    }

    clearTables(sqlContext.sparkSession, securityLevel, loadTables)
    result
  }
}
