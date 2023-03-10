package com.coupang.search.pk

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.sellmerfud.optparse._
import com.coupang.search.pk.utils.S3Utils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import com.coupang.search.pk.utils.IoUtil._
import com.coupang.search.pk.utils.EntityUtil._
import org.apache.spark.sql.Row

/** * Calculating entity coverage for queries.
  */
object CalculateQueryStoreEntityCoverage {
  val QUERY_JOIN_TABLE = "indexing_platform.query_join_v2"
  // the following table is from store query entity table, queries's entity is extracted realtime by Henry's team
  val SEARCH_QUERY_JOIN_TABLE =
    "search.features_join_v2_with_cached_request_multi_task"

  lazy val logger: Logger =
    LogManager.getLogger(CalculateQueryStoreEntityCoverage.getClass)

  case class Config(
      ratingCountThreshold: Option[Int] = None,
      output: String = ""
  )

  def udfGetGenericEntityNames: UserDefinedFunction = udf(
    getGenericEntityNames _
  )

  def udfGetTopProductsNames: UserDefinedFunction = udf(
    getTopProductNames _
  )
  
  def udfGetStoreQueryEntities: UserDefinedFunction = udf(
    getGenericEntityNames _
  )

  // def udfGetStoreQueryEntitiesStats: UserDefinedFunction = udf(
  //   getStoreQueryEntitiesStats _
  // )

  // def getStoreQueryEntitiesStats(storeQueryEntity: Seq[Row]): StoreQueryEntityResult = {
  //   if (storeQueryEntity == null || storeQueryEntity.isEmpty) {
  //     return StoreQueryEntityResult()
  //   }

  //     val storeQueryModelEntityExposes2w: Option[Integer] = None
  //     val storeQueryModelEntityPurchases2w: Option[Integer] = None
  //     val storeQueryBrandEntityExposes3m: Option[Integer] = None
  //     val storeQueryBrandEntityPurchases3m: Option[Integer] = None
  //     val storeQueryBrandEntityClicks3m: Option[Integer] = None
  //     val storeQueryFlavorEntityClicks2w: Option[Integer] = None
  //     val storeQueryPackageTypeEntityClicks2w: Option[Integer] = None
  //     val storeQueryTypeEntityExposes2w: Option[Integer] = None
  //     val storeQueryTypeEntityPurchases2w: Option[Integer] = None
  //     val storeQueryCustomerEntityQueryFeature3m: Option[Integer] = None
  //     val solrCustomerClickedEntityPreferenceRealtime: Option[Integer] = None
  //     val solrCustomerQueryEntityPreference3m: Option[Integer] = None
  //     val solrCustomerPurchasedEntityPreference: Option[Integer] = None
  //     val solrCustomerClickedEntityPreference: Option[Integer] = None

  //   storeQueryEntity.foreach((row: Row) => {
  //       storeQueryModelEntityExposes2w = if (!row.isNullAt(0)) {
  //         Some(row.getInt(0))
  //       } else {
  //         None
  //       }

  //       storeQueryModelEntityPurchases2w = if (!row.isNullAt(0)) {
  //         Some(row.getInt(1))
  //       } else {
  //         None
  //       }

  //       storeQueryBrandEntityExposes3m = if (!row.isNullAt(0)) {
  //         Some(row.getInt(2))
  //       } else {
  //         null
  //       }

  //       storeQueryBrandEntityPurchases3m = if (!row.isNullAt(0)) {
  //         Some(row.getInt(3))
  //       } else {
  //         null
  //       }

  //       storeQueryBrandEntityClicks3m = if (!row.isNullAt(0)) {
  //         Some(row.getInt(4))
  //       } else {
  //         null
  //       }

  //       storeQueryFlavorEntityClicks2w = if (!row.isNullAt(0)) {
  //         Some(row.getInt(5))
  //       } else {
  //         null
  //       }

  //       storeQueryPackageTypeEntityClicks2w = if (!row.isNullAt(0)) {
  //         Some(row.getInt(6))
  //       } else {
  //         null
  //       }

  //       storeQueryTypeEntityExposes2w = if (!row.isNullAt(0)) {
  //         Some(row.getInt(7))
  //       } else {
  //         null
  //       }

  //       storeQueryTypeEntityPurchases2w = if (!row.isNullAt(0)) {
  //         Some(row.getInt(8))
  //       } else {
  //         null
  //       }

  //       storeQueryCustomerEntityQueryFeature3m = if (!row.isNullAt(0)) {
  //         Some(row.getInt(9))
  //       } else {
  //         null
  //       }

  //       solrCustomerClickedEntityPreferenceRealtime = if (!row.isNullAt(0)) {
  //         Some(row.getInt(10))
  //       } else {
  //         null
  //       }

  //       solrCustomerQueryEntityPreference3m = if (!row.isNullAt(0)) {
  //         Some(row.getInt(11))
  //       } else {
  //         null
  //       }

  //       solrCustomerPurchasedEntityPreference = if (!row.isNullAt(0)) {
  //         Some(row.getInt(12))
  //       } else {
  //         null
  //       }

  //       solrCustomerClickedEntityPreference = if (!row.isNullAt(0)) {
  //         Some(row.getInt(13))
  //       } else {
  //         null
  //       }
        
  //   })
  //   StoreQueryEntityResult(
  //     storeQueryModelEntityExposes2w,
  //     storeQueryModelEntityPurchases2w,
  //     storeQueryBrandEntityExposes3m,
  //     storeQueryBrandEntityClicks3m,
  //     storeQueryBrandEntityClicks3m,
  //     storeQueryFlavorEntityClicks2w,
  //     storeQueryPackageTypeEntityClicks2w,
  //     storeQueryTypeEntityExposes2w,
  //     storeQueryTypeEntityPurchases2w,
  //     storeQueryCustomerEntityQueryFeature3mr,
  //     solrCustomerClickedEntityPreferenceReanteger,
  //     solrCustomerQueryEntityPreference3m,
  //     solrCustomerPurchasedEntityPreference,
  //     solrCustomerClickedEntityPreference
  //     )
  // }

  def getEntityResult(dfQeuryEntity: DataFrame): DataFrame = {
    val dfEntityResult = dfQeuryEntity
      .withColumn(
        "genericEntityResult",
        udfGetGenericEntityNames(col("generic_entities"))
      )
      .withColumn(
        "versionedTaxonomySignal",
        udfGetTaxonomySignal(col("versioned_taxonomy_signal"))
      )
      .withColumn(
        "topProducts1m",
        udfGetTopProductsNames(col("top_products_1m"))
      )
      // .withColumn(
      //   "storeQueryEntity",
      //   udfGetStoreQueryEntitiesStats(
      //     col("query_model_entity_exposes_2w"),
      //     col("query_model_entity_purchases_2w"),
      //     col("query_brand_entity_purchases_2w"),
      //     col("query_brand_entity_exposes_3m"),
      //     col("query_brand_entity_clicks_3m"),
      //     col("query_flavor_entity_clicks_2w"),
      //     col("query_package_type_entity_clicks_2w"),
      //     col("query_type_entity_purchases_2w"),
      //     col("query_type_entity_exposes_2w"),
      //     col("customer_entity_query_feature_3m"),
      //     col("solr_customer_clicked_entity_preference_realtime"),
      //     col("solr_customer_clicked_entity_preference"),
      //     col("solr_customer_query_entity_preference_3m"),
      //     col("solr_customer_purchased_entity_preference")
      //   )
      // )
      .withColumn(
        "queryEntities",
        udfGetStoreQueryEntities(
          col("query_entities")
        )
      )
      .select(
        "query",
        "genericEntityResult.*",
        "topProducts1m.*",
        "versionedTaxonomySignal.*",
        "storeQueryEntity.*"
      )
    dfEntityResult
  }

  def parseArgs(args: Array[String]): Config = {
    val config =
      try {
        new OptionParser[Config] {
          reqd[String]("", "--output", "Output base path") { (v, cfg) =>
            cfg.copy(output = v)
          }
        }.parse(args, Config())
      } catch {
        case e: OptionParserException => println(e.getMessage); sys.exit(1)
      }
    config
  }

  /** * Getting query entity information from query_join_v2
    * @param spark
    *   SparkSession object
    * @return
    *   A DataFrame of columns "query", and other entity columns
    */
  def getQueryEntity()(implicit spark: SparkSession): DataFrame = {
    spark
      .table(QUERY_JOIN_TABLE)
      .filter(col("dt") === "latest")
      .select(
        col("query"),
        col("query_top_products.top_products_1m").as("top_products_1m"),
        col("taxonomy_signal.versioned_taxonomy_signal")
          .as("versioned_taxonomy_signal"),
        col("generic_entities")
      )
  }

  def getSearchQueryJoinTable()(implicit spark: SparkSession): DataFrame = {
    spark
      .table(SEARCH_QUERY_JOIN_TABLE)
      .filter(col("dt") == "latest")
      .select(
        col("query"),
        col("query_entities"),
        col("query_model_entity_exposes_2w"),
        col("query_model_entity_purchases_2w"),
        col("query_brand_entity_purchases_2w"),
        col("query_brand_entity_exposes_3m"),
        col("query_brand_entity_clicks_3m"),
        col("query_flavor_entity_clicks_2w"),
        col("query_package_type_entity_clicks_2w"),
        col("query_type_entity_purchases_2w"),
        col("query_type_entity_exposes_2w"),
        col("customer_entity_query_feature_3m"),
        col("solr_customer_clicked_entity_preference_realtime"),
        col("solr_customer_clicked_entity_preference"),
        col("solr_customer_query_entity_preference_3m"),
        col("solr_customer_purchased_entity_preference")
      )
  }

  def udfGetTaxonomySignal: UserDefinedFunction = udf(getTaxonomySignal _)
  def getSampleOutputPath(output: String): String = {
    S3Utils.joinPath(output, "samples")
  }

  /**
   [info] compiling 3 Scala sources to /Users/dili/Documents/workspace/clean_spark_product_knowledge/spark-product-knowledge/apps/analysis/target/scala-2.11/classes ...
[info] 2 file(s) merged using strategy 'First' (Run the task at debug level to see the details)
[info] 125 file(s) merged using strategy 'Discard' (Run the task at debug level to see the details)
[error] /Users/dili/Documents/workspace/clean_spark_product_knowledge/spark-product-knowledge/apps/analysis/src/main/scala/com/coupang/search/pk/CalculateQueryStoreEntityCoverage.scala:255:8: overloaded method value filter with alternatives:
[error]   (func: org.apache.spark.api.java.function.FilterFunction[org.apache.spark.sql.Row])org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] <and>
[error]   (func: org.apache.spark.sql.Row => Boolean)org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] <and>
[error]   (conditionExpr: String)org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] <and>
[error]   (condition: org.apache.spark.sql.Column)org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]
[error]  cannot be applied to (Boolean)
[error]       .filter(col("dt") == "latest")
[error]        ^
[error] one error found
[info] Built: /Users/dili/Documents/workspace/clean_spark_product_knowledge/spark-product-knowledge/core/target/scala-2.11/pk-core-assembly-1.0.jar
[info] Jar hash: caab540b11f9471e1801a061918089680ad884e5
[error] (analysis / Compile / compileIncremental) Compilation failed
[error] Total time: 12 s, completed Mar 9, 2023, 6:05:06 PM
   *
  /
  def main(args: Array[String]): Unit = {
    val config = parseArgs(args)
    implicit val spark: SparkSession =
      SparkSession.builder.enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    logger.info("Getting store entity...")
    val dfQueryEntity = getQueryEntity()
    val dfSearchQuery = getSearchQueryJoinTable()
    val dfJoined = dfSearchQuery.join(dfQueryEntity, Seq("query"))
    val dfEntityResult = getEntityResult(dfJoined)
    val dfSample = dfEntityResult.sample(0.001).limit(1000)
    val totalRecords = dfEntityResult.count()
    val coverageCols = dfEntityResult.columns
      .map(c => count(col(c)).divide(lit(totalRecords)).as(c))

    val dfCoverage = dfEntityResult.select(
      coverageCols :+ lit(totalRecords).as("total_records"): _*
    )

    dfCoverage.saveAsOneCsv(config.output, "coverage")
    dfSample
      .coalesce(1)
      .write
      .option("header", value = true)
      .mode(SaveMode.Overwrite)
      .save(getSampleOutputPath(config.output))
  }
}
