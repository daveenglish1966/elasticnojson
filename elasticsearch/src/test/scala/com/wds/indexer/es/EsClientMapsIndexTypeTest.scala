package com.wds.indexer.es

import java.util
import java.util.concurrent.atomic.AtomicInteger

import com.wds.indexer.es.helpers.EsMap
import com.wds.utils.MapSOPrinter
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.rest.RestStatus
import org.scalatest.{FunSuite, _}


class EsClientMapsIndexTypeTest extends FunSuite with Matchers with EsClientTestBase {

  val esClientIndexTypeMaps = new EsClientMapsIndexType(new EsClientMaps(esClient),
                                                        indexName,
                                                        indexType)

  test("EsClientTestBase initialzation verification") {
    esCluster should not be null
    esClient should not be null
    esClientIndex should not be null

    println(s"esClient: ${esClient.toString}")
  }

  test("EsClientMapsIndexTypeTest test ready") {
    esClientIndexTypeMaps should not be null
  }

  test("test all gets") {
    resetIndex()

    val numberOfItems = 10
    val list = randomMaps(numberOfItems)
    val results = esClientIndexTypeMaps.upsert(list.iterator)
    results.hasFailures should be (false)
    sleep(1)
    esClientIndexTypeMaps.count should be (numberOfItems)

    val esMap0 = esClientIndexTypeMaps.get(list(0).id)
    mapsAreEqual(list(0).map, esMap0.map) should be (true)

    val esMap1 = esClientIndexTypeMaps.get(QueryBuilders.matchQuery("firstName", list(1).map.get("firstName")))
    mapsAreEqual(list(1).map, esMap1.map) should be (true)

    val mapOfList: Map[String, EsMap] = list2Map(list)
    val all = esClientIndexTypeMaps.getList
    all.size should be (numberOfItems)
    all.foreach{ esMap =>
      //println("checking: " + esMap.id)
      mapOfList.contains(esMap.id) should be (true)
    }

    val all2 = esClientIndexTypeMaps.getList(mapOfList.keys.iterator)
    all2.size should be (numberOfItems)
    all2.foreach{ esMap =>
      //println("checking: " + esMap.id)
      mapOfList.contains(esMap.id) should be (true)
    }

    val srb = esClientIndexTypeMaps.createSearchRequestBuilder()
    srb.setQuery(QueryBuilders.matchQuery("firstName", list(0).map.get("firstName") + " " + list(1).map.get("firstName")))
    val some = esClientIndexTypeMaps.getList(srb)
    some.size should be (2)
  }

  test("test upsert from scratch") {
    resetIndex()
    val numberOfItems = 100

    val list = randomMaps(numberOfItems)
    val results = esClientIndexTypeMaps.upsert(list.iterator)
    results.hasFailures should be (false)
    sleep(1)
    esClientIndexTypeMaps.count should be (numberOfItems)

    esClientIndexTypeMaps.deleteByIds(list.map(esMap => esMap.id).iterator)
    sleep(1)
    esClientIndexTypeMaps.count should be (0)
  }

  test("test update and upsert lists") {
    resetIndex()
    val numberOfItems = 100

    val list = randomMaps(numberOfItems)
    val results = esClientIndexTypeMaps.insert(list.iterator)
    results.hasFailures should be (false)
    sleep(1)
    esClientIndexTypeMaps.count should be (numberOfItems)

    val javaMap = new util.HashMap[String, AnyRef]()
    javaMap.put("upsertKey", "upsertValue")
    esClientIndexTypeMaps.upsert(list.map(esMap => new EsMap(esMap.id, javaMap)).iterator)
    sleep(1)
    esClientIndexTypeMaps.count should be (numberOfItems)

    val brb2 = esClientIndexTypeMaps.delete(list.iterator)
    brb2.hasFailures should be (false)
    sleep(1)
    esClientIndexTypeMaps.count should be (0)
  }

  /**
    * This test prooves that only the insert will reset the
    * document.  Both update and upsert will just append the
    * changes in the map provided.
    */
  test("test update single subtleties") {
    resetIndex()

    val esMap = randomMap

    esClientIndexTypeMaps.insert(esMap)
    sleep(1)
    esClientIndexTypeMaps.count should be (1)

    testMapInjection("upsert", esMap.id, esMap => esClientIndexTypeMaps.upsert(esMap))
    testMapInjection("update", esMap.id, esMap => esClientIndexTypeMaps.update(esMap))
    testMapInjection("insert", esMap.id, esMap => esClientIndexTypeMaps.insert(esMap))

    esClientIndexTypeMaps.deleteById(esMap.id)
    sleep(1)
    esClientIndexTypeMaps.count should be(0)
  }

  private def testMapInjection(test: String,
                               id: String,
                               consumer: (EsMap) => Unit): Unit = {
    val javaMap = new java.util.HashMap[String, AnyRef]
    javaMap.put(test + "Key", test + "Value");
    consumer(new EsMap(id, javaMap))
    sleep(1)
    esClientIndexTypeMaps.count should be(1)
    val retEsMap = esClientIndexTypeMaps.get(id)
    println(test + " map\n" + MapSOPrinter.printMap("  ", retEsMap.map))
  }

  /**
    * def count(indexName: String, indexType: String): Long
    * def count(indexName: String, indexType: String, queryBuilder: QueryBuilder): Long
    *
    * def insert(indexName: String, indexType: String, map: util.Map[String, AnyRef]): IndexResponse
    * def insert(indexName: String, indexType: String, esMap: EsMap): IndexResponse
    * def insert(indexName: String, indexType: String, mapIter: Iterator[EsMap]): BrbResults
    */
  test("inserts and counts") {
    resetIndex()

    val javaMap = new java.util.HashMap[String, Object]
    javaMap.put("firstName", "David")
    javaMap.put("lastName", "English")

    val ir = esClientIndexTypeMaps.insert(javaMap)
    ir.status should be (RestStatus.CREATED)
    println("ir(1): " + ir.toString)
    sleep(1)
    esClientIndexTypeMaps.count should be (1)

    val esMap = new EsMap(ir.getId, javaMap)
    val ir2 = esClientIndexTypeMaps.insert(esMap)
    ir2.status should be (RestStatus.OK)
    println("ir(2): " + ir2.toString)
    sleep(1)
    esClientIndexTypeMaps.count should be (1)

    val numberToInsert = 1000
    val list = randomMaps(numberToInsert)
    val brbr = esClientIndexTypeMaps.insert(list.iterator)
    sleep(1)
    brbr.hasFailures should be (false)
    esClientIndexTypeMaps.count should be (1+numberToInsert)

    val count = new AtomicInteger(0);
    list.foreach(esMap => {
      val fn = esMap.map.get("firstName")
      esClientIndexTypeMaps.count(QueryBuilders.matchQuery("firstName", fn)) should be (1)
    })
  }

  /**
    * def count(indexName: String, indexType: String): Long
    *
    * def delete(indexName: String, indexType: String, esMap: EsMap): DeleteResponse
    * def deleteById(indexName: String, indexType: String, id: String): DeleteResponse
    * def delete(indexName: String, indexType: String, listIter: Iterator[EsMap]): BrbResults
    * def deleteByIds(indexName: String, indexType: String, listIter: Iterator[String]): BrbResults
    */
  test("deletes") {
    resetIndex()

    val esMap1 = randomMap
    val ir1 = esClientIndexTypeMaps.insert(esMap1.map)
    sleep(1)
    ir1.status should be (RestStatus.CREATED)
    esClientIndexTypeMaps.count should be (1)

    val esMap2 = randomMap
    val ir2 = esClientIndexTypeMaps.insert(esMap2)
    sleep(1)
    ir2.status should be (RestStatus.CREATED)
    esClientIndexTypeMaps.count should be (2)

    val dr1 = esClientIndexTypeMaps.deleteById(ir1.getId)
    dr1.status should be (RestStatus.OK)
    val dr2 = esClientIndexTypeMaps.delete(esMap2)
    dr2.status should be (RestStatus.OK)
    sleep(1)
    esClientIndexTypeMaps.count should be (0)

    val numberOfObjs = 10

    val rol1 = randomMaps(numberOfObjs)
    val brbrs1 = esClientIndexTypeMaps.insert(rol1.iterator)
    brbrs1.hasFailures should be (false)
    sleep(1)
    esClientIndexTypeMaps.count should be (numberOfObjs)
    esClientIndexTypeMaps.deleteByIds(rol1.map(esMap => esMap.id).iterator)
    sleep(1)
    esClientIndexTypeMaps.count should be (0)

    val rol2 = randomMaps(numberOfObjs)
    val brbrs2 = esClientIndexTypeMaps.insert(rol2.iterator)
    brbrs2.hasFailures should be (false)
    sleep(1)
    esClientIndexTypeMaps.count should be (numberOfObjs)
    esClientIndexTypeMaps.delete(rol2.iterator)
    sleep(1)
    esClientIndexTypeMaps.count should be (0)
  }
}
