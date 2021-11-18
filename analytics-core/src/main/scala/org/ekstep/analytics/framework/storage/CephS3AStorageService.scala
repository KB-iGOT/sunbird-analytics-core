package org.ekstep.analytics.framework.storage

import org.ekstep.analytics.framework.conf.AppConf
import org.jclouds.ContextBuilder
import org.jclouds.blobstore.{BlobStore, BlobStoreContext}
import org.jclouds.s3.reference.S3Constants
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.Model.Blob
import org.sunbird.cloud.storage.factory.StorageConfig

// import java.util.Properties


class CephS3AStorageService(config: StorageConfig) extends BaseStorageService {
  // var overrides = new Properties()
  // overrides.setProperty(S3Constants.PROPERTY_S3_VIRTUAL_HOST_BUCKETS, AppConf.getConfig("s3_like_path_style_access"))
  // overrides.setProperty("jclouds.regions", AppConf.getConfig("s3_like_region"))
//  var context: BlobStoreContext = ContextBuilder.newBuilder("aws-s3")
//    .endpoint(AppConf.getConfig("s3_like_endpoint"))
//    .credentials(config.storageKey, config.storageSecret)
//    // .overrides(overrides)
//    .buildView(classOf[BlobStoreContext])
//

  var context: BlobStoreContext = ContextBuilder.newBuilder("s3").endpoint(config.endPoint.get).credentials(config.storageKey, config.storageSecret).buildView(classOf[BlobStoreContext])
  var blobStore: BlobStore = context.getBlobStore

  override def getPaths(container: String, objects: List[Blob]): List[String] = {
    objects.map{f => "s3n://" + container + "/" + f.key}
  }
}