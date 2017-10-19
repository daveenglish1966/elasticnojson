package com.wds.indexer.es

import java.util
import java.util.concurrent.atomic.AtomicInteger

import com.wds.indexer.es.helpers.EsMap
import com.wds.utils.MapSOPrinter
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.rest.RestStatus
import org.scalatest.{FunSuite, _}

class EsClientMapsTest extends FunSuite with Matchers with EsClientTestBase {

  val esClientMaps = new EsClientMaps(esClient)

  test("EsClientTestBase initialzation verification") {
    esCluster should not be null
    esClient should not be null
    esClientIndex should not be null

    println(s"esClient: ${esClient.toString}")
  }

  test("EsClientMapsTest initialzation verification") {
    esClientMaps should not be null
  }

  test("test Map") {
    val map = Map("key1" -> "value1",
                  "key2" -> "value2",
                  "key3" -> "value3",
                  "key4" -> "value4",
                  "key5" -> "value5")
    map contains "key1" should be (true)
    map.foreach{ e =>
      println("e._1: " + e._1)
      println("  e._2: " + e._2)
      map contains e._1 should be (true)
    }
  }

  test("test all gets") {
    resetIndex()

    val numberOfItems = 10
    var list = randomMaps(numberOfItems)
    val results = esClientMaps.upsert(indexName, indexType, list.iterator)
    results.hasFailures should be (false)
    sleep(1)
    esClientMaps.count(indexName, indexType) should be (numberOfItems)

    val esMap0 = esClientMaps.get(indexName, indexType, list(0).id)
    mapsAreEqual(list(0).map, esMap0.map) should be (true)

    val esMap1 = esClientMaps.get(indexName, indexType, QueryBuilders.matchQuery("firstName", list(1).map.get("firstName")))
    mapsAreEqual(list(1).map, esMap1.map) should be (true)

    val mapOfList: Map[String, EsMap] = list2Map(list)
    val all = esClientMaps.getList(indexName, indexType)
    all.size should be (numberOfItems)
    all.foreach{ esMap =>
      //println("checking: " + esMap.id)
      mapOfList.contains(esMap.id) should be (true)
    }

    val all2 = esClientMaps.getList(indexName, indexType, mapOfList.keys.iterator)
    all2.size should be (numberOfItems)
    all2.foreach{ esMap =>
      //println("checking: " + esMap.id)
      mapOfList.contains(esMap.id) should be (true)
    }

    val srb = esClientMaps.createSearchRequestBuilder(indexName, indexType)
    srb.setQuery(QueryBuilders.matchQuery("firstName", list(0).map.get("firstName") + " " + list(1).map.get("firstName")))
    val some = esClientMaps.getList(srb)
    some.size should be (2)
  }

  test("test upsert from scratch") {
    resetIndex()
    val numberOfItems = 100

    val list = randomMaps(numberOfItems)
    val results = esClientMaps.upsert(indexName, indexType, list.iterator)
    results.hasFailures should be (false)
    sleep(1)
    esClientMaps.count(indexName, indexType) should be (numberOfItems)

    esClientMaps.deleteByIds(indexName, indexType, list.map(esMap => esMap.id).iterator)
    sleep(1)
    esClientMaps.count(indexName, indexType) should be (0)
  }

  test("test update and upsert lists") {
    resetIndex()
    val numberOfItems = 100

    val list = randomMaps(numberOfItems)
    val results = esClientMaps.insert(indexName, indexType, list.iterator)
    results.hasFailures should be (false)
    sleep(1)
    esClientMaps.count(indexName, indexType) should be (numberOfItems)

    val javaMap = new util.HashMap[String, AnyRef]()
    javaMap.put("upsertKey", "upsertValue")
    esClientMaps.upsert(indexName, indexType, list.map(esMap => new EsMap(esMap.id, javaMap)).iterator)
    sleep(1)
    esClientMaps.count(indexName, indexType) should be (numberOfItems)

    var brb2 = esClientMaps.delete(indexName, indexType, list.iterator)
    brb2.hasFailures should be (false)
    sleep(1)
    esClientMaps.count(indexName, indexType) should be (0)
  }

  /**
  * This test prooves that only the insert will reset the
  * document.  Both update and upsert will just append the
  * changes in the map provided.
  */
  test("test update single subtleties") {
    resetIndex()

    val esMap = randomMap

    esClientMaps.insert(indexName, indexType, esMap)
    sleep(1)
    esClientMaps.count(indexName, indexType) should be (1)

    testMapInjection("upsert", esMap.id, esMap => esClientMaps.upsert(indexName, indexType, esMap))
    testMapInjection("update", esMap.id, esMap => esClientMaps.update(indexName, indexType, esMap))
    testMapInjection("insert", esMap.id, esMap => esClientMaps.insert(indexName, indexType, esMap))

    esClientMaps.deleteById(indexName, indexType, esMap.id)
    sleep(1)
    esClientMaps.count(indexName, indexType) should be(0)
  }

  private def testMapInjection(test: String,
                               id: String,
                               consumer: (EsMap) => Unit): Unit = {
    val javaMap = new java.util.HashMap[String, AnyRef]
    javaMap.put(test + "Key", test + "Value");
    consumer(new EsMap(id, javaMap))
    sleep(1)
    esClientMaps.count(indexName, indexType) should be(1)
    val retEsMap = esClientMaps.get(indexName, indexType, id)
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

    var ir = esClientMaps.insert(indexName, indexType, javaMap)
    ir.status should be (RestStatus.CREATED)
    println("ir(1): " + ir.toString)
    sleep(1)
    esClientMaps.count(indexName, indexType) should be (1)

    val esMap = new EsMap(ir.getId, javaMap)
    ir = esClientMaps.insert(indexName, indexType, esMap)
    ir.status should be (RestStatus.OK)
    println("ir(2): " + ir.toString)
    sleep(1)
    esClientMaps.count(indexName, indexType) should be (1)

    val numberToInsert = 1000
    val list = randomMaps(numberToInsert)
    val brbr = esClientMaps.insert(indexName, indexType, list.iterator)
    sleep(1)
    brbr.hasFailures should be (false)
    esClientMaps.count(indexName, indexType) should be (1+numberToInsert)

    var count = new AtomicInteger(0);
    list.foreach(esMap => {
      val fn = esMap.map.get("firstName")
      esClientMaps.count(indexName, indexType, QueryBuilders.matchQuery("firstName", fn)) should be (1)
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
    val ir1 = esClientMaps.insert(indexName, indexType, esMap1.map)
    sleep(1)
    ir1.status should be (RestStatus.CREATED)
    esClientMaps.count(indexName, indexType) should be (1)

    val esMap2 = randomMap
    val ir2 = esClientMaps.insert(indexName, indexType, esMap2)
    sleep(1)
    ir2.status should be (RestStatus.CREATED)
    esClientMaps.count(indexName, indexType) should be (2)

    val dr1 = esClientMaps.deleteById(indexName, indexType, ir1.getId)
    dr1.status should be (RestStatus.OK)
    val dr2 = esClientMaps.delete(indexName, indexType, esMap2)
    dr2.status should be (RestStatus.OK)
    sleep(1)
    esClientMaps.count(indexName, indexType) should be (0)

    val numberOfObjs = 10

    val rol1 = randomMaps(numberOfObjs)
    val brbrs1 = esClientMaps.insert(indexName, indexType, rol1.iterator)
    brbrs1.hasFailures should be (false)
    sleep(1)
    esClientMaps.count(indexName, indexType) should be (numberOfObjs)
    esClientMaps.deleteByIds(indexName, indexType, rol1.map(esMap => esMap.id).iterator)
    sleep(1)
    esClientMaps.count(indexName, indexType) should be (0)

    val rol2 = randomMaps(numberOfObjs)
    val brbrs2 = esClientMaps.insert(indexName, indexType, rol2.iterator)
    brbrs2.hasFailures should be (false)
    sleep(1)
    esClientMaps.count(indexName, indexType) should be (numberOfObjs)
    esClientMaps.delete(indexName, indexType, rol2.iterator)
    sleep(1)
    esClientMaps.count(indexName, indexType) should be (0)
  }
}
