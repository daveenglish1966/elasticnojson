package com.wds.indexer.es

import java.util.UUID

import com.wds.indexer.es.helpers.EsMap
import com.wds.indexer.es.meta.{EsCluster, HostPort}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


trait EsClientTestBase {

  val indexName = "test-index"
  val indexType = "test-type"
  val esCluster = new EsCluster("docker-cluster", List(new HostPort("localhost", 9300)))
  val esClient = new EsClient(esCluster)
  val esClientIndex = new EsClientIndex(esClient)

  protected def sleep(seconds: Int) = Thread.sleep(seconds*1000)

  protected def randomMaps(numberOfItems: Int) : List[EsMap] = {
    val listBuffer = new ListBuffer[EsMap]
    for(i <- 0 until numberOfItems) {
      listBuffer += randomMap
    }
    listBuffer.toList
  }

  protected def randomMap(): EsMap =
  {
    val javaMap = new java.util.HashMap[String, Object]
    javaMap.put("firstName", uniqueStr)
    javaMap.put("lastName", uniqueStr)
    new EsMap(uniqueStr, javaMap)
  }

  protected def uniqueStr: String = UUID.randomUUID().toString.replace("-", "")

  protected def resetIndex() = {
    if (esClientIndex.indexExists(indexName)) {
      esClientIndex.deleteIndex(indexName)
    }

    esClientIndex.createIndex(indexName, indexType)
  }

  protected def list2Map(esMaps: List[EsMap]) = {
    val mmap = mutable.Map[String, EsMap]()
    esMaps.foreach{ esMap =>
      mmap += (esMap.id -> esMap)
    }
    mmap.toMap
  }

  protected def mapsAreEqual(map1: java.util.Map[String, AnyRef],
                             map2: java.util.Map[String, AnyRef]) : Boolean =
    return map1.get("firstName") == map2.get("firstName") &&
           map1.get("lastName") == map2.get("lastName")
}
