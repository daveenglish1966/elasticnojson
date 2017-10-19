package com.wds.indexer.es

import java.io.IOException
import java.util

import org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder
import org.slf4j.LoggerFactory

class EsClientIndex(val esClient: EsClient) {
  private val LOG = LoggerFactory.getLogger(classOf[EsClientIndex])

  def indexExists(indexName: String): Boolean = esClient.client
                                                        .admin
                                                        .indices
                                                        .prepareExists(indexName)
                                                        .execute
                                                        .actionGet
                                                        .isExists

  def deleteIndex(indexName: String): Boolean = {
    val dib = esClient.client.admin.indices.prepareDelete(indexName)
    val dir = dib.execute.actionGet
    dir.isAcknowledged
  }

  def createIndex(indexName: String,
                  indexType: String,
                  numerOfReplicas: Int = 0,
                  numberOfShards: Int = 1) = createIndexAndMappings(indexName, indexType, numerOfReplicas, numberOfShards)

  def createIndexAndMappings(indexName: String,
                             indexType: String,
                             numerOfReplicas: Int,
                             numberOfShards: Int,
                             list: util.Map[String, AnyRef]*): Boolean = {
    val cirb = esClient.client.admin.indices.prepareCreate(indexName)
    try
      cirb.setSettings(jsonBuilder.startObject
                                  .field("number_of_replicas", numerOfReplicas)
                                  .field("number_of_shards", numberOfShards)
                                  .endObject)
    catch {
      case e: IOException =>
        LOG.error("UNEXPECTED ERROR MAKING SETTINGS", e)
        return false
    }
    LOG.info("Creating index " + indexName + "/" + indexType)
    for (map <- list) {
      cirb.addMapping(indexType, map)
    }
    val cir = cirb.execute.actionGet
    val isAcknowledged = cir.isAcknowledged
    LOG.info("isAcknowledged: " + isAcknowledged)
    isAcknowledged
  }

}
