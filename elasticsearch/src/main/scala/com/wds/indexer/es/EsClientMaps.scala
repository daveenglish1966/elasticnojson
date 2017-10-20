package com.wds.indexer.es

import java.io.IOException
import java.util
import java.util.concurrent.ExecutionException

import com.wds.indexer.es.helpers.{BrbResults, EsMap}
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.index.{IndexRequest, IndexResponse}
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.action.update.{UpdateRequest, UpdateResponse}
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

class EsClientMaps(esClient: EsClient) extends EsClientScanner(esClient) {
  private val LOG = LoggerFactory.getLogger(classOf[EsClientScanner])

  @throws[ExecutionException]
  @throws[InterruptedException]
  def count(indexName: String,
            indexType: String): Long = count(indexName, indexType, null)

  @throws[ExecutionException]
  @throws[InterruptedException]
  def count(indexName: String,
            indexType: String,
            queryBuilder: QueryBuilder): Long = {
    val searchResponse = esClient.client
                                 .prepareSearch(indexName)
                                 .setTypes(indexType)
                                 .setQuery(queryBuilder)
                                 .setFrom(0)
                                 .setSize(0)
                                 .execute
                                 .get
    return searchResponse.getHits.getTotalHits
  }

  //==========================================================================
  // insert
  // https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-docs-index.html
  //==========================================================================
  def insert(indexName: String,
             indexType: String,
             map: util.Map[String, AnyRef]): IndexResponse = {
    esClient.client
            .prepareIndex(indexName, indexType)
            .setSource(map)
            .get
  }

  def insert(indexName: String,
             indexType: String,
             esMap: EsMap): IndexResponse = {
    esClient.client.prepareIndex(indexName,
                                 indexType,
                                 esMap.id)
      .setSource(esMap.map)
      .get
  }

  def insert(indexName: String,
             indexType: String,
             mapIter: Iterator[EsMap]): BrbResults = {
    val brb = esClient.client.prepareBulk
    for (esMap <- mapIter) {
      brb.add(esClient.client.prepareIndex(indexName,
                                           indexType,
                                           esMap.id)
        .setSource(esMap.map))
    }
    new BrbResults(brb.get)
  }

  //==========================================================================
  // update
  // https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-docs-update.html
  //==========================================================================
  def update(indexName: String,
             indexType: String,
             esMap: EsMap): UpdateResponse = {
    val updateRequest = new UpdateRequest(indexName, indexType, esMap.id).doc(esMap.map)
    esClient.client.update(updateRequest).get
  }

  def update(indexName: String,
             indexType: String,
             listIter: Iterator[EsMap]): BrbResults = {
    val brb = esClient.client.prepareBulk
    for (esMap <- listIter) {
      brb.add(new UpdateRequest(indexName, indexType, esMap.id).doc(esMap.map))
    }
    new BrbResults(brb.get)
  }

  //==========================================================================
  // upsert
  // https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-docs-update.html
  //==========================================================================
  def upsert(indexName: String,
             indexType: String,
             esMap: EsMap): UpdateResponse = {
    val indexRequest = new IndexRequest(indexName, indexType, esMap.id).source(esMap.map)
    val updateRequest = new UpdateRequest(indexName, indexType, esMap.id).doc(esMap.map).upsert(indexRequest)
    esClient.client.update(updateRequest).get
  }

  def upsert(indexName: String,
             indexType: String,
             listIter: Iterator[EsMap]): BrbResults = {
    val brb = esClient.client.prepareBulk
    for (esMap <- listIter) {
      val indexRequest = new IndexRequest(indexName, indexType, esMap.id).source(esMap.map)
      val updateRequest = new UpdateRequest(indexName, indexType, esMap.id).doc(esMap.map).upsert(indexRequest)
      brb.add(updateRequest)
    }
    new BrbResults(brb.get)
  }

  //==========================================================================
  // delete
  // https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-docs-delete.html
  //==========================================================================
  def delete(indexName: String,
             indexType: String,
             esMap: EsMap): DeleteResponse = {
    deleteById(indexName, indexType, esMap.id)
  }

  def deleteById(indexName: String,
                 indexType: String,
                 id: String): DeleteResponse = {
    esClient.client.prepareDelete(indexName, indexType, id).get
  }

  def delete(indexName: String,
             indexType: String,
             listIter: Iterator[EsMap]): BrbResults = {
    val listBuffer = new ListBuffer[String]
    for(esMap <- listIter) {
      listBuffer += esMap.id
    }
    deleteByIds(indexName, indexType, listBuffer.toList.iterator)
  }

  def deleteByIds(indexName: String,
                  indexType: String,
                  listIter: Iterator[String]): BrbResults = {
    if (listIter.isEmpty) return null
    val brb = esClient.client.prepareBulk
    for (id <- listIter) {
      brb.add(esClient.client.prepareDelete(indexName, indexType, id))
    }
    new BrbResults(brb.get)
  }

  //==========================================================================
  // get
  //==========================================================================
  @throws[IOException]
  def get(indexName: String,
          indexType: String,
          id: String): EsMap = {
    val getResponse = esClient.client.prepareGet(indexName, indexType, id).get
    new EsMap(getResponse.getId, getResponse.getSource)
  }

  @throws[ExecutionException]
  @throws[InterruptedException]
  def get(indexName: String,
          indexType: String,
          queryBuilder: QueryBuilder): EsMap = {
    val srb = createSearchRequestBuilder(indexName, indexType).setQuery(queryBuilder)
    val listBuffer = new ListBuffer[EsMap]
    val addToList = (esMap: EsMap) => listBuffer += esMap : Unit
    iterateResults(srb, addToList, TimeValue.timeValueMinutes(2), 2)

    val list = listBuffer.toList
    if (list.size == 0)
      return null
    if (list.size > 1)
      throw new IllegalArgumentException("More that one result found.")
    list.head
  }

  //==========================================================================
  // getList
  //==========================================================================
  @throws[ExecutionException]
  @throws[InterruptedException]
  def getList(indexName: String,
              indexType: String): Iterator[EsMap] = {
    val srb = createSearchRequestBuilder(indexName, indexType)
    val listBuffer = new ListBuffer[EsMap]
    val addToList = (esMap: EsMap) => listBuffer += esMap : Unit
    iterateResults(srb, addToList)
    listBuffer.toList.iterator
  }

  @throws[ExecutionException]
  @throws[InterruptedException]
  def getList(indexName: String,
              indexType: String,
              ids: Iterator[String]): Iterator[EsMap] = {
    val srb = createSearchRequestBuilder(indexName, indexType)
                 .setQuery(QueryBuilders.idsQuery()
                                        .addIds(ids.toArray: _*))
    getList(srb)
  }

  @throws[ExecutionException]
  @throws[InterruptedException]
  def getList(srb: SearchRequestBuilder): Iterator[EsMap] = {
    val listBuffer = new ListBuffer[EsMap]
    val addToList = (esMap: EsMap) => listBuffer += esMap : Unit
    iterateResults(srb, addToList)
    listBuffer.toList.iterator
  }

  //==========================================================================
  // getNotify
  //==========================================================================
  @throws[ExecutionException]
  @throws[InterruptedException]
  @inline def getNotify(indexName: String,
                        indexType: String,
                        consumer: (EsMap) => Unit): Unit = {
    val srb = createSearchRequestBuilder(indexName, indexType)
    iterateResults(srb, consumer)
  }

  @throws[ExecutionException]
  @throws[InterruptedException]
  def getNotify(indexName: String,
                indexType: String,
                ids: Iterator[String],
                consumer: (EsMap) => Unit): Unit = {
    val srb = createSearchRequestBuilder(indexName, indexType)
      .setQuery(QueryBuilders.idsQuery().addIds(ids.toArray: _*))
    getNotify(srb, consumer)
  }

  @throws[ExecutionException]
  @throws[InterruptedException]
  @inline def getNotify(srb: SearchRequestBuilder,
                        consumer: (EsMap) => Unit): Unit = {
    iterateResults(srb, consumer)
  }

  @throws[ExecutionException]
  @throws[InterruptedException]
  @inline def getNotify(indexName: String,
                        indexType: String,
                        queryBuilder: QueryBuilder,
                        consumer: (EsMap) => Unit): Unit = {
    val srb = createSearchRequestBuilder(indexName, indexType).setQuery(queryBuilder)
    iterateResults(srb, consumer)
  }
}
