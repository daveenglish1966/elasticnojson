package com.wds.indexer.es

import java.io.IOException
import java.util.concurrent.ExecutionException

import com.wds.indexer.es.helpers.{BrbResults, EsMap, EsObj}
import com.wds.indexer.es.iterator.{MapIterator, ObjIterator}
import com.wds.utils.mapping.MapObj
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchType}
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilder

class EsClientTypes[T](val esClientMaps: EsClientMaps,
                       val indexName: String,
                       val indexType: String,
                       val tClazz: Class[T])
{
  val esClientMapsIndexType = new EsClientMapsIndexType(esClientMaps, indexName, indexType)

  @inline def createSearchRequestBuilder(searchType: SearchType = SearchType.DEFAULT) =
    esClientMapsIndexType.createSearchRequestBuilder(searchType)

  @throws[ExecutionException]
  @throws[InterruptedException]
  @inline def count : Long =
    esClientMapsIndexType.count
  @throws[ExecutionException]
  @throws[InterruptedException]
  @inline def count(queryBuilder: QueryBuilder) : Long =
    esClientMapsIndexType.count(queryBuilder)

  @inline def insert(esObj: EsObj[T]): IndexResponse =
    esClientMapsIndexType.insert(new EsMap(esObj.id, MapObj.obj2Map(esObj.data)))
  @inline def insert(iterator: Iterator[EsObj[T]]): BrbResults =
    esClientMapsIndexType.insert(new MapIterator(iterator))

  @inline def update(esObj: EsObj[T]): UpdateResponse =
    esClientMapsIndexType.update(new EsMap(esObj.id, MapObj.obj2Map(esObj.data)))
  @inline def update(iterator: Iterator[EsObj[T]]): BrbResults =
    esClientMapsIndexType.update(new MapIterator(iterator))

  @inline def upsert(esObj: EsObj[T]): UpdateResponse =
    esClientMapsIndexType.upsert(new EsMap(esObj.id, MapObj.obj2Map(esObj.data)))
  @inline def upsert(iterator: Iterator[EsObj[T]]): BrbResults =
    esClientMapsIndexType.upsert(new MapIterator(iterator))

  @inline def delete(esObj: EsObj[T]): DeleteResponse =
    esClientMapsIndexType.delete(new EsMap(esObj.id, MapObj.obj2Map(esObj.data)))
  @inline def deleteById(id: String): DeleteResponse =
    esClientMapsIndexType.deleteById(id)
  @inline def delete(iterator: Iterator[EsObj[T]]): BrbResults =
    esClientMapsIndexType.delete(new MapIterator(iterator))
  @inline def deleteByIds(iterator: Iterator[String]): BrbResults =
    esClientMapsIndexType.deleteByIds(iterator)

  @throws[IOException]
  @inline def get(id: String): EsObj[T] = {
    val esMap: EsMap = esClientMapsIndexType.get(id)
    new EsObj(esMap.id, MapObj.map2Obj(esMap.map, tClazz))
  }
  @throws[ExecutionException]
  @throws[InterruptedException]
  @inline def get(queryBuilder: QueryBuilder): EsObj[T] = {
    val esMap = esClientMapsIndexType.get(queryBuilder)
    new EsObj(esMap.id, MapObj.map2Obj(esMap.map, tClazz))
  }

  @throws[ExecutionException]
  @throws[InterruptedException]
  @inline def getList: Iterator[EsObj[T]] =
    new ObjIterator[T](esClientMapsIndexType.getList, tClazz)
  @throws[ExecutionException]
  @throws[InterruptedException]
  @inline def getList(ids: Iterator[String]): Iterator[EsObj[T]] =
    new ObjIterator[T](esClientMapsIndexType.getList(ids), tClazz)
  @throws[ExecutionException]
  @throws[InterruptedException]
  @inline def getList(srb: SearchRequestBuilder): Iterator[EsObj[T]] =
    new ObjIterator[T](esClientMapsIndexType.getList(srb), tClazz)

  @inline
  def scan(consumer: (EsMap) => Unit,
           queryBuilder: QueryBuilder = null,
           fields: Array[String] = null,
           searchType: SearchType = SearchType.DEFAULT,
           scrollFetchSize: Int = 1000,
           scrollTimeout: TimeValue = TimeValue.timeValueMinutes(2),
           quitAfter: Long = 0): Unit = esClientMapsIndexType.scan(consumer,
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
  @inline def getNotify(consumer: (EsObj[T]) => Unit): Unit = {
    val esMapConsumer = (esMap: EsMap) => consumer(new EsObj(esMap.id, MapObj.map2Obj(esMap.map, tClazz))) : Unit
    esClientMapsIndexType.getNotify(esMapConsumer)
  }

  @throws[ExecutionException]
  @throws[InterruptedException]
  @inline def getNotify(ids: Iterator[String],
                        consumer: (EsObj[T]) => Unit): Unit = {
    val esMapConsumer = (esMap: EsMap) => consumer(new EsObj(esMap.id, MapObj.map2Obj(esMap.map, tClazz))): Unit
    esClientMapsIndexType.getNotify(ids, esMapConsumer)
  }

  @throws[ExecutionException]
  @throws[InterruptedException]
  @inline def getNotify(srb: SearchRequestBuilder,
                        consumer: (EsObj[T]) => Unit): Unit = {
    val esMapConsumer = (esMap: EsMap) => consumer(new EsObj(esMap.id, MapObj.map2Obj(esMap.map, tClazz))): Unit
    esClientMapsIndexType.getNotify(srb, esMapConsumer)
  }

  @throws[ExecutionException]
  @throws[InterruptedException]
  @inline def getNotify(indexName: String,
                        indexType: String,
                        queryBuilder: QueryBuilder,
                        consumer: (EsObj[T]) => Unit): Unit = {
    val esMapConsumer = (esMap: EsMap) => consumer(new EsObj(esMap.id, MapObj.map2Obj(esMap.map, tClazz))): Unit
    esClientMapsIndexType.getNotify(queryBuilder, esMapConsumer)
  }
}
