package com.coupang.search.pk

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode, SparkSession}
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

   def udfGetStoreQueryEntitiesStats: UserDefinedFunction = udf(
     getStoreQueryEntitiesStats _
   )

  case class StoreQueryEntityResult(
                                     storeQueryModelEntityExposes2w: Option[Int] = None,
                                     storeQueryModelEntityPurchases2w: Option[Int] = None,
                                     storeQueryBrandEntityExposes3m: Option[Int] = None,
                                     storeQueryBrandEntityPurchases3m: Option[Int] = None,
                                     storeQueryBrandEntityClicks3m: Option[Int] = None,
                                     storeQueryFlavorEntityClicks2w: Option[Int] = None,
                                     storeQueryPackageTypeEntityClicks2w: Option[Int] = None,
                                     storeQueryTypeEntityExposes2w: Option[Int] = None,
                                     storeQueryTypeEntityPurchases2w: Option[Int] = None,
                                     storeQueryCustomerEntityQueryFeature3m: Option[Int] = None,
                                     solrCustomerClickedEntityPreferenceRealtime: Option[Int] = None,
                                     solrCustomerQueryEntityPreference3m: Option[Int] = None,
                                     solrCustomerPurchasedEntityPreference: Option[Int] = None,
                                     solrCustomerClickedEntityPreference: Option[Int] = None
                                   )

  def getStoreQueryEntitiesStats(storeQueryEntity: Seq[Row]): StoreQueryEntityResult = {
    if (storeQueryEntity == null || storeQueryEntity.isEmpty) {
      return StoreQueryEntityResult()
    }

    var storeQueryModelEntityExposes2w: Option[Int] = None
    var storeQueryModelEntityPurchases2w: Option[Int] = None
    var storeQueryBrandEntityExposes3m: Option[Int] = None
    var storeQueryBrandEntityPurchases3m: Option[Int] = None
    var storeQueryBrandEntityClicks3m: Option[Int] = None
    var storeQueryFlavorEntityClicks2w: Option[Int] = None
    var storeQueryPackageTypeEntityClicks2w: Option[Int] = None
    var storeQueryTypeEntityExposes2w: Option[Int] = None
    var storeQueryTypeEntityPurchases2w: Option[Int] = None
    var storeQueryCustomerEntityQueryFeature3m: Option[Int] = None
    var solrCustomerClickedEntityPreferenceRealtime: Option[Int] = None
    var solrCustomerQueryEntityPreference3m: Option[Int] = None
    var solrCustomerPurchasedEntityPreference: Option[Int] = None
    var solrCustomerClickedEntityPreference: Option[Int] = None

    storeQueryEntity.foreach((row: Row) => {
      storeQueryModelEntityExposes2w = Option(row.getInt(0))
      storeQueryModelEntityPurchases2w = Option(row.getInt(1))
      storeQueryBrandEntityExposes3m = Option(row.getInt(2))
      storeQueryBrandEntityPurchases3m = Option(row.getInt(3))
      storeQueryBrandEntityClicks3m = Option(row.getInt(4))
      storeQueryFlavorEntityClicks2w = Option(row.getInt(5))
      storeQueryPackageTypeEntityClicks2w = Option(row.getInt(6))
      storeQueryTypeEntityExposes2w = Option(row.getInt(7))
      storeQueryTypeEntityPurchases2w = Option(row.getInt(8))
      storeQueryCustomerEntityQueryFeature3m = Option(row.getInt(9))
      solrCustomerClickedEntityPreferenceRealtime = Option(row.getInt(10))
      solrCustomerQueryEntityPreference3m = Option(row.getInt(11))
      solrCustomerPurchasedEntityPreference = Option(row.getInt(12))
      solrCustomerClickedEntityPreference = Option(row.getInt(13))
    }

     StoreQueryEntityResult(
       storeQueryModelEntityExposes2w,
       storeQueryModelEntityPurchases2w,
       storeQueryBrandEntityExposes3m,
       storeQueryBrandEntityClicks3m,
       storeQueryBrandEntityClicks3m,
       storeQueryFlavorEntityClicks2w,
       storeQueryPackageTypeEntityClicks2w,
       storeQueryTypeEntityExposes2w,
       storeQueryTypeEntityPurchases2w,
       storeQueryCustomerEntityQueryFeature3mr,
       solrCustomerClickedEntityPreferenceReanteger,
       solrCustomerQueryEntityPreference3m,
       solrCustomerPurchasedEntityPreference,
       solrCustomerClickedEntityPreference
       )
   }

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
      .withColumn(
        "storeQueryEntity",
        udfGetStoreQueryEntitiesStats(
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
      )
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
      .filter(col("dt").equalTo("latest"))
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
