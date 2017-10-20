package com.wds.indexer.es

import java.io.IOException
import java.util
import java.util.concurrent.ExecutionException

import com.wds.indexer.es.helpers.{BrbResults, EsMap}
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchType}
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}

class EsClientMapsIndexType(val esClientMaps: EsClientMaps,
                            val indexName: String,
                            val indexType: String) {
  @throws[ExecutionException]
  @throws[InterruptedException]
  @inline def count() : Long =
    esClientMaps.count(indexName, indexType)
  @throws[ExecutionException]
  @throws[InterruptedException]
  @inline def count(queryBuilder: QueryBuilder) : Long =
    esClientMaps.count(indexName, indexType, queryBuilder)

  @inline def insert(map: util.Map[String, AnyRef]): IndexResponse =
    esClientMaps.insert(indexName, indexType, map)
  @inline def insert(map: EsMap): IndexResponse =
    esClientMaps.insert(indexName, indexType, map)
  @inline def insert(list: Iterator[EsMap]): BrbResults =
    esClientMaps.insert(indexName, indexType, list)

  @inline def update(map: EsMap): UpdateResponse =
    esClientMaps.update(indexName, indexType, map)
  @inline def update(list: Iterator[EsMap]): BrbResults =
    esClientMaps.update(indexName, indexType, list)

  @inline def upsert(map: EsMap): UpdateResponse =
    esClientMaps.upsert(indexName, indexType, map)
  @inline def upsert(list: Iterator[EsMap]): BrbResults =
    esClientMaps.upsert(indexName, indexType, list)

  @inline def delete(map: EsMap): DeleteResponse =
    esClientMaps.delete(indexName, indexType, map)
  @inline def deleteById(id: String): DeleteResponse =
    esClientMaps.deleteById(indexName, indexType, id)
  @inline def delete(list: Iterator[EsMap]): BrbResults =
    esClientMaps.delete(indexName, indexType, list)
  @inline def deleteByIds(list: Iterator[String]): BrbResults =
    esClientMaps.deleteByIds(indexName, indexType, list)

  @throws[IOException]
  @inline def get(id: String): EsMap =
    esClientMaps.get(indexName, indexType, id)
  @throws[ExecutionException]
  @throws[InterruptedException]
  @inline def get(queryBuilder: QueryBuilder): EsMap =
    esClientMaps.get(indexName, indexType, queryBuilder)

  @throws[ExecutionException]
  @throws[InterruptedException]
  @inline def getList(): Iterator[EsMap] =
    esClientMaps.getList(indexName, indexType)
  @throws[ExecutionException]
  @throws[InterruptedException]
  @inline def getList(ids: Iterator[String]): Iterator[EsMap] =
    esClientMaps.getList(indexName, indexType, ids)
  @throws[ExecutionException]
  @throws[InterruptedException]
  @inline def getList(srb: SearchRequestBuilder): Iterator[EsMap] =
    esClientMaps.getList(srb)

  @inline def createSearchRequestBuilder(searchType: SearchType = SearchType.DEFAULT) = 
    esClientMaps.createSearchRequestBuilder(indexName, indexType, searchType)

  @inline
  def scan(consumer: (EsMap) => Unit,
           queryBuilder: QueryBuilder = null,
           fields: Array[String] = null,
           searchType: SearchType = SearchType.DEFAULT,
           scrollFetchSize: Int = 1000,
           scrollTimeout: TimeValue = TimeValue.timeValueMinutes(2),
           quitAfter: Long = 0): Unit = esClientMaps.scan(indexName,
                                                          indexType,
                                                          consumer,
                                                          queryBuilder,
                                                          fields,
                                                          searchType,
                                                          scrollFetchSize,
                                                          scrollTimeout,
                                                          quitAfter)

  //==========================================================================
  // getNotify
  //==========================================================================
  @throws[ExecutionException]
  @throws[InterruptedException]
  @inline def getNotify(consumer: (EsMap) => Unit): Unit =
    esClientMaps.getNotify(indexName, indexType, consumer)

  @throws[ExecutionException]
  @throws[InterruptedException]
  @inline def getNotify(ids: Iterator[String],
                        consumer: (EsMap) => Unit): Unit =
    esClientMaps.getNotify(indexName, indexType, ids, consumer)

  @throws[ExecutionException]
  @throws[InterruptedException]
  @inline def getNotify(srb: SearchRequestBuilder,
                        consumer: (EsMap) => Unit): Unit =
    esClientMaps.getNotify(srb, consumer)

  @throws[ExecutionException]
  @throws[InterruptedException]
  @inline def getNotify(queryBuilder: QueryBuilder,
                        consumer: (EsMap) => Unit): Unit =
    esClientMaps.getNotify(indexName, indexType, queryBuilder, consumer)
}
