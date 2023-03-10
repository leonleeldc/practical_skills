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

def getStoreQueryEntities(storeQueryEntity: Seq[Row]): StoreQueryEntityResult = {
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
    storeQueryModelEntityExposes2w = if (!row.isNullAt(0)) {
      Some(row.getInt(0))
    } else {
      None
    }

    storeQueryModelEntityPurchases2w = if (!row.isNullAt(1)) {
      Some(row.getInt(1))
   }
  }
}

import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode, SparkSession}

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

def getStoreQueryEntities(storeQueryEntity: Seq[Row]): StoreQueryEntityResult = {
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
    solr
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