package com.wds.indexer.es.helpers

import org.apache.commons.lang3.builder.{EqualsBuilder, HashCodeBuilder}

class EsObj[T](val id: String,
               val data: T) {
  def canEqual(any: Any) = any.isInstanceOf[EsObj[T]]
  override def equals(that: Any): Boolean =
    that match {
      case that: EsObj[T] => that.canEqual(this) &&
        new EqualsBuilder()
          .append(this.id, that.id)
          .append(this.data, that.data)
          .isEquals()
      case _ => false
    }
  override def hashCode: Int =
    return new HashCodeBuilder()
      .append(id)
      .append(data)
      .toHashCode()
}
