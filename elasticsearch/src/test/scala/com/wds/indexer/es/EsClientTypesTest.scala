package com.wds.indexer.es

import java.util.concurrent.atomic.AtomicInteger

import com.wds.indexer.es.helpers.EsObj
import org.apache.commons.lang3.builder.{EqualsBuilder, HashCodeBuilder}
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.rest.RestStatus
import org.scalatest.{FunSuite, _}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class Person(firstName: String,
                  lastName: String) extends Serializable {
  override def toString = firstName + " " + lastName
  def canEqual(any: Any) = any.isInstanceOf[Person]
  override def equals(that: Any): Boolean =
    that match {
      case that: Person => that.canEqual(this) &&
        new EqualsBuilder()
          .append(this.firstName,that.firstName)
          .append(this.lastName, that.lastName)
          .isEquals()
      case _ => false
    }
  override def hashCode: Int =
    return new HashCodeBuilder()
      .append(firstName)
      .append(lastName)
      .toHashCode()
}

class EsPerson(id: String, data: Person) extends EsObj[Person](id, data)

class EsClientTypesTest extends FunSuite with Matchers with EsClientTestBase {

  val esClientTypes = new EsClientTypes(new EsClientMaps(esClient),
                                        indexName,
                                        indexType,
                                        classOf[Person])

  test("EsClientTestBase initialzation verification") {
    esCluster should not be null
    esClient should not be null
    esClientIndex should not be null

    println(s"esClient: ${esClient.toString}")
  }

  test("esClientTypes validate setup") {
    esClientTypes should not be null
  }

  test("test all gets") {
    resetIndex()

    val numberOfItems = 10
    var list = randomObjects(numberOfItems)
    val results = esClientTypes.upsert(list.iterator)
    results.hasFailures should be (false)
    sleep(1)
    esClientTypes.count should be (numberOfItems)

    val esObj0 = esClientTypes.get(list(0).id)
    assert(list(0) === esObj0)

    val esObj1 = esClientTypes.get(QueryBuilders.matchQuery("firstName", list(1).data.firstName))
    assert(list(1) === esObj1)

    val mapOfList: Map[String, EsObj[Person]] = list2EsObjMap(list)
    val all = esClientTypes.getList
    all.size should be (numberOfItems)
    all.foreach{ esMap =>
      //println("checking: " + esMap.id)
      mapOfList.contains(esMap.id) should be (true)
    }

    val all2 = esClientTypes.getList(mapOfList.keys.iterator)
    all2.size should be (numberOfItems)
    all2.foreach{ esMap =>
      //println("checking: " + esMap.id)
      mapOfList.contains(esMap.id) should be (true)
    }

    val srb = esClientTypes.createSearchRequestBuilder()
    srb.setQuery(QueryBuilders.matchQuery("firstName", list(0).data.firstName + " " + list(1).data.firstName))
    val some = esClientTypes.getList(srb)
    some.size should be (2)
  }

  test("test upsert from scratch") {
    resetIndex()
    val numberOfItems = 100

    val list = randomObjects(numberOfItems)
    val results = esClientTypes.upsert(list.iterator)
    results.hasFailures should be (false)
    sleep(1)
    esClientTypes.count should be (numberOfItems)

    esClientTypes.deleteByIds(list.map(esMap => esMap.id).iterator)
    sleep(1)
    esClientTypes.count should be (0)
  }

  test("test update and upsert lists") {
    resetIndex()
    val numberOfItems = 100

    val list = randomObjects(numberOfItems)
    val results = esClientTypes.insert(list.iterator)
    results.hasFailures should be (false)
    sleep(1)
    esClientTypes.count should be (numberOfItems)

    var brb2 = esClientTypes.delete(list.iterator)
    brb2.hasFailures should be (false)
    sleep(1)
    esClientTypes.count should be (0)
  }

  /**
    * This test prooves that only the insert will reset the
    * document.  Both update and upsert will just append the
    * changes in the map provided.
    */
  test("test update single subtleties") {
    resetIndex()

    val esObj = randomObject

    esClientTypes.insert(esObj)
    sleep(1)
    esClientTypes.count should be (1)

    testMapInjection("upsert", esObj.id, esObj => esClientTypes.upsert(esObj))
    testMapInjection("update", esObj.id, esObj => esClientTypes.update(esObj))
    testMapInjection("insert", esObj.id, esObj => esClientTypes.insert(esObj))

    esClientTypes.deleteById(esObj.id)
    sleep(1)
    esClientTypes.count should be(0)
  }

  private def testMapInjection(test: String,
                               id: String,
                               consumer: (EsObj[Person]) => Unit): Unit = {
    println("test: " + test)
    val person = randomPerson
    val esObj = new EsObj(id, person)
    consumer(esObj)
    sleep(1)
    esClientTypes.count should be(1)
    val retEsObj = esClientTypes.get(id)
    println(test + " data\n" + retEsObj.data)
  }

  /**
    * def count(indexName: String, indexType: String): Long
    * def count(indexName: String, indexType: String, queryBuilder: QueryBuilder): Long
    *
    * def insert(indexName: String, indexType: String, map: util.Map[String, AnyRef]): IndexResponse
    * def insert(indexName: String, indexType: String, esMap: EsObj): IndexResponse
    * def insert(indexName: String, indexType: String, mapIter: Iterator[EsObj]): BrbResults
    */
  test("inserts and counts") {
    resetIndex()

    val esPerson = randomObject
    var ir = esClientTypes.insert(esPerson)
    ir.status should be (RestStatus.CREATED)
    println("ir(1): " + ir.toString)
    sleep(1)
    esClientTypes.count should be (1)

    val esPerson2 = new EsObj(ir.getId, esPerson.data)
    ir = esClientTypes.insert(esPerson2)
    ir.status should be (RestStatus.OK)
    println("ir(2): " + ir.toString)
    sleep(1)
    esClientTypes.count should be (1)

    val numberToInsert = 1000
    val list = randomObjects(numberToInsert)
    val brbr = esClientTypes.insert(list.iterator)
    sleep(1)
    brbr.hasFailures should be (false)
    esClientTypes.count should be (1+numberToInsert)

    var count = new AtomicInteger(0);
    list.foreach(esMap => {
      val fn = esMap.data.firstName
      esClientTypes.count(QueryBuilders.matchQuery("firstName", fn)) should be (1)
    })
  }

  /**
    * def count(indexName: String, indexType: String): Long
    *
    * def delete(indexName: String, indexType: String, esMap: EsObj): DeleteResponse
    * def deleteById(indexName: String, indexType: String, id: String): DeleteResponse
    * def delete(indexName: String, indexType: String, listIter: Iterator[EsObj]): BrbResults
    * def deleteByIds(indexName: String, indexType: String, listIter: Iterator[String]): BrbResults
    */
  test("deletes") {
    resetIndex()

    val esPerson1 = randomObject
    val ir1 = esClientTypes.insert(esPerson1)
    sleep(1)
    ir1.status should be (RestStatus.CREATED)
    esClientTypes.count should be (1)

    val esMap2 = randomObject
    val ir2 = esClientTypes.insert(esMap2)
    sleep(1)
    ir2.status should be (RestStatus.CREATED)
    esClientTypes.count should be (2)

    val dr1 = esClientTypes.deleteById(ir1.getId)
    dr1.status should be (RestStatus.OK)
    val dr2 = esClientTypes.delete(esMap2)
    dr2.status should be (RestStatus.OK)
    sleep(1)
    esClientTypes.count should be (0)

    val numberOfObjs = 10

    val rol1 = randomObjects(numberOfObjs)
    val brbrs1 = esClientTypes.insert(rol1.iterator)
    brbrs1.hasFailures should be (false)
    sleep(1)
    esClientTypes.count should be (numberOfObjs)
    esClientTypes.deleteByIds(rol1.map(esMap => esMap.id).iterator)
    sleep(1)
    esClientTypes.count should be (0)

    val rol2 = randomObjects(numberOfObjs)
    val brbrs2 = esClientTypes.insert(rol2.iterator)
    brbrs2.hasFailures should be (false)
    sleep(1)
    esClientTypes.count should be (numberOfObjs)
    esClientTypes.delete(rol2.iterator)
    sleep(1)
    esClientTypes.count should be (0)
  }
  private def randomObjects(numberOfItems: Int) : List[EsObj[Person]] = {
    val listBuffer = new ListBuffer[EsObj[Person]]
    for(i <- 0 until numberOfItems) {
      listBuffer += randomObject
    }
    listBuffer.toList
  }

  private def randomObject(): EsObj[Person] =
    new EsObj(uniqueStr, randomPerson)

  private def randomPerson(): Person =
    new Person(uniqueStr, uniqueStr)

  private def list2EsObjMap(esMaps: List[EsObj[Person]]) = {
    val mmap = mutable.Map[String, EsObj[Person]]()
    esMaps.foreach{ esObj =>
      mmap += (esObj.id -> esObj)
    }
    mmap.toMap
  }
}
