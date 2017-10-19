package com.wds.indexer.es.helpers

import com.wds.utils.MapSOPrinter
import org.apache.commons.lang3.StringUtils
import org.elasticsearch.search.SearchHit

class EsMap(val id: String,
            val map: java.util.Map[String, AnyRef]) {
  require(!StringUtils.isEmpty(id))
  require(map != null)
  def this(searchHit: SearchHit) = this(searchHit.getId, searchHit.getSourceAsMap)
  override def toString = id + ":\n" + MapSOPrinter.printMap("  ", map)
}
