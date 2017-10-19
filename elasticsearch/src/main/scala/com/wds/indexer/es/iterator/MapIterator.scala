package com.wds.indexer.es.iterator

import com.wds.indexer.es.helpers.{EsMap, EsObj}
import com.wds.utils.mapping.MapObj

class MapIterator[T](val iterator: Iterator[EsObj[T]]) extends Iterator[EsMap] {
  val esMapClazz = classOf[EsMap]
  @inline override def hasNext = iterator.hasNext
  @inline override def next() = {
    val esObj: EsObj[T] = iterator.next()
    new EsMap(esObj.id, MapObj.obj2Map(esObj.data))
  }
}
