package test

import org.scalatest.{FunSuite, Matchers}

class TestTest extends FunSuite with Matchers {

  var test1Run = false;
  var test2Run = false;
  var test3Run = false;
  test("test 1") {
    println("test 1")
    test1Run = true;
  }
  test("test 2") {
    println("test 2")
    test2Run = true;
    assert(test1Run === true)
  }
  test("test 3") {
    test3Run = true;
    println("test 3")
    assert(test2Run === true)
  }
  test("test 4") {
    println("test 4")
    assert(test3Run === true)
  }
  test("Parallel Array Traversal") {
    val list1 = List(1, 2, 3)
    val list2 = List(4, 5, 6)
    val list2Iter = list2.iterator
    for(i1 <- list1) {
      println(s"i1=$i1 i2=${list2Iter.next()}")
    }
  }
}
