package com.wds.indexer.es

import com.wds.indexer.es.helpers.EsMap
import org.scalatest.{FunSuite, _}

import scala.collection.mutable.ListBuffer


class EsClientScannerTest extends FunSuite with Matchers with EsClientTestBase {

  val esClientScanner = new EsClientMapsIndexType(new EsClientMaps(esClient), indexName, indexType)

  test("EsClientTestBase initialzation verification") {
    esCluster should not be null
    esClient should not be null
    esClientIndex should not be null

    println(s"esClient: ${esClient.toString}")
  }

  test("esClientIndexTypeMaps initialzation verification") {
    esClientScanner should not be null
  }

  test("test all gets") {
    resetIndex()

    // Create list of items
    val numberOfItems = 100000
    val list = randomMaps(numberOfItems)
    val brbr = esClientScanner.insert(list.iterator)
    brbr.hasFailures should be (false)
    sleep(10)
    esClientScanner.count should be (numberOfItems)

    // Scan your 100,000 (numberOfItems) objects here.
    val listBuffer = new ListBuffer[EsMap]
    val addToList = (esMap: EsMap) => listBuffer += esMap : Unit
    esClientScanner.scan(addToList)
    val scannedList = listBuffer.toList

    // See if everything maps
    scannedList.size should be (numberOfItems)
    val listMap = list2Map(list)
    val scannedListMap = list2Map(scannedList)
    listMap.foreach{ esMap =>
      scannedListMap.contains(esMap._1) should be (true)
    }

    // Cleanup of list
    esClientScanner.delete(list.iterator)
    sleep(10)
    esClientScanner.count should be (0)
  }
}
