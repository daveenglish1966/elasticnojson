package com.wds.utils.mapping

import com.wds.utils.MapSOPrinter
import org.scalatest.{FunSuite, Matchers}

class Person(val firstName: String,
             val lastName: String,
             var id: String = "")

class MapObjTest extends FunSuite with Matchers {
  test("Basic Serialization to Map") {
    val person = new Person("Dave", "English")
    val map = MapObj.obj2Map(person)
    println("map type name: " + map.getClass.getName)
    println(MapSOPrinter.printMap(map))

    val person2 = MapObj.map2Obj(map, classOf[Person])
    assert(person.firstName == person2.firstName)
    assert(person.lastName == person2.lastName)
  }
}
