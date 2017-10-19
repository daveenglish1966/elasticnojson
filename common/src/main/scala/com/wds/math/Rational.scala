package com.wds.math

import org.apache.commons.lang3.builder.{EqualsBuilder, HashCodeBuilder}

class Rational( n: Int, d: Int) {
  require(d != 0)
  private val g = gcd( n.abs, d.abs)
  val numer = n / g
  val denom = d / g
  def this(n: Int) = this( n, 1)

  // Add
  def + (that: Rational): Rational =
    new Rational(numer * that.denom + that.numer * denom,
                 denom * that.denom )
  def + (i: Int): Rational =
    new Rational(numer + i * denom, denom)

  // Substract
  def - (that: Rational): Rational =
    new Rational(numer * that.denom - that.numer * denom,
                 denom * that.denom )
  def - (i: Int): Rational = new Rational( numer - i * denom, denom)

  // Multiply
  def * (that: Rational): Rational =
    new Rational(numer * that.numer,
                 denom * that.denom)
  def * (i: Int): Rational =
    new Rational(numer * i, denom)

  // Divide
  def / (that: Rational): Rational =
    new Rational(numer * that.denom,
                 denom * that.numer)
  def / (i: Int): Rational =
    new Rational( numer, denom * i)

  // Overrides
  override def toString = numer + "/" + denom
  def canEqual(any: Any) = any.isInstanceOf[Rational]
  override def equals(that: Any): Boolean =
    that match {
      case that: Rational => that.canEqual(this) && new EqualsBuilder().append(this.numer, that.numer)
                                                                       .append(this.denom, that.denom)
                                                                       .isEquals()
      case _ => false
    }
  override def hashCode: Int =
    return new HashCodeBuilder().append(numer)
                                .append(denom)
                                .toHashCode()

  // Private Methods
  private def gcd( a: Int, b: Int): Int = if (b == 0) a else gcd( b, a % b)
}

object Rational {
  implicit def intToRational(i: Int) = new Rational(i)
}
