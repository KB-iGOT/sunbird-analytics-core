package org.ekstep.analytics.framework.storage

import org.jclouds.ContextBuilder
import org.jclouds.blobstore.{BlobStore, BlobStoreContext}
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.Model.Blob

import java.util.Properties


class S3LikeStorageService(config: S3LikeStorageConfig) extends BaseStorageService {
  var overrides = new Properties()
  overrides.setProperty("PROPERTY_S3_VIRTUAL_HOST_BUCKETS", config.pathStyleAccess)

  var context: BlobStoreContext = ContextBuilder.newBuilder("aws-s3")
    .endpoint(config.endpoint)
    .credentials(config.storageKey, config.storageSecret)
    .overrides(overrides)
    .buildView(classOf[BlobStoreContext])
  var blobStore: BlobStore = context.getBlobStore

  override def getPaths(container: String, objects: List[Blob]): List[String] = {
    objects.map{f => "s3n://" + container + "/" + f.key}
  }
}