package org.ekstep.analytics.framework.storage

import org.sunbird.cloud.storage.factory.StorageConfig

case class S3LikeStorageConfig(
                                endpoint: scala.Predef.String,
                                override val `type` : scala.Predef.String,
                                override val storageKey : scala.Predef.String,
                                override val storageSecret : scala.Predef.String,
                                pathStyleAccess: scala.Predef.String
                              ) extends StorageConfig(`type`, storageKey, storageSecret)

