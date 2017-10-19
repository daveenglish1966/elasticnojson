package com.wds.indexer.es.iterator

import com.wds.indexer.es.helpers.{EsMap, EsObj}
import com.wds.utils.mapping.MapObj

class ObjIterator[T](val iterator:Iterator[EsMap],
                     val tClazz: Class[T]) extends Iterator[EsObj[T]] {
  override def hasNext = iterator.hasNext
  override def next() = {
    val esMap: EsMap = iterator.next()
    new EsObj(esMap.id, MapObj.map2Obj(esMap.map, tClazz))
  }
}
