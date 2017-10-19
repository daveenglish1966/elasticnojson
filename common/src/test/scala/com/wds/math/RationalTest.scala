package com.wds.math

import org.scalatest.FunSuite

class RationalTest extends FunSuite {

  test("Hello World Test") {
    println("Hello World");
  }

  test("1/2 + 1/2 = 1") {
    val oneHalf1 = new Rational(1,2)
    val oneHalf2 = new Rational(2,4)
    val whole = oneHalf1 + oneHalf2
    assert(whole == new Rational(1))
  }

  test("1/2 * 1/2 = 1/4") {
    val oneHalf1 = new Rational(1,2)
    val oneHalf2 = new Rational(2,4)
    val whole = oneHalf1 * oneHalf2
    assert(whole == new Rational(1,4))
  }
}
