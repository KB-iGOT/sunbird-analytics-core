package org.ekstep.analytics.framework.fetcher

import org.ekstep.analytics.framework.{FrameworkContext, Query}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.exception.DataFetcherException

object GcloudDataFetcher {

  @throws(classOf[DataFetcherException])
  def getObjectKeys(queries: Array[Query])(implicit fc: FrameworkContext): Array[String] = {

    val keys = for(query <- queries) yield {
      val paths = if(query.folder.isDefined && query.endDate.isDefined && query.folder.getOrElse("false").equals("true")) {
        Array("gs://" + getBucket(query.bucket) + "/" + getPrefix(query.prefix) + query.endDate.getOrElse(""))
      } else {
        getKeys(query)
      }
      if(query.excludePrefix.isDefined) {
        paths.filter { x => !x.contains(query.excludePrefix.get) }
      } else {
        paths
      }
    }
    keys.flatMap { x => x.map { x => x } }
  }

  private def getKeys(query: Query)(implicit fc: FrameworkContext): Array[String] = {
    val storageService = fc.getStorageService("gcloud", "storage.key.config", "storage.secret.config")
    println("Got query" + query)
    println("Got storage Service" + storageService)
    val keys = storageService.searchObjects(getBucket(query.bucket), getPrefix(query.prefix), query.startDate, query.endDate, query.delta, query.datePattern.getOrElse("yyyy-MM-dd"))
    println("found keys" + keys)

    val paths = storageService.getPaths(getBucket(query.bucket), keys).toArray
    println("found paths" + paths)
    paths
  }

  private def getBucket(bucket: Option[String]): String = {
    bucket.getOrElse("telemetry-data-store");
  }

  private def getPrefix(prefix: Option[String]): String = {
    prefix.getOrElse("raw/")
  }
}
