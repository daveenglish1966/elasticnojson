package com.wds.indexer.es

import java.util.concurrent.ExecutionException
import java.util.concurrent.atomic.AtomicLong

import com.wds.indexer.es.helpers.EsMap
import org.elasticsearch.action.search._
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilder
import org.slf4j.LoggerFactory

import scala.annotation.tailrec

class EsClientScanner(val esClient: EsClient) {
  private val LOG = LoggerFactory.getLogger(classOf[EsClientScanner])

  def createSearchRequestBuilder(indexName: String,
                                 indexType: String,
                                 searchType: SearchType = SearchType.DEFAULT) = esClient.client
                                                                        .prepareSearch(indexName)
                                                                        .setTypes(indexType)
                                                                        .setSearchType(searchType)

  @throws[ExecutionException]
  @throws[InterruptedException]
  def scan(indexName: String,
           indexType: String,
           consumer: (EsMap) => Unit,
           queryBuilder: QueryBuilder = null,
           fields: Array[String] = null,
           searchType: SearchType = SearchType.DEFAULT,
           scrollFetchSize: Int = 1000,
           scrollTimeout: TimeValue = TimeValue.timeValueMinutes(2),
           quitAfter: Long = 0): Unit = {
    val searchRequestBuilder: SearchRequestBuilder = createSearchRequestBuilder(indexName,
                                                                                indexType,
                                                                                searchType)
    if (queryBuilder != null)
      searchRequestBuilder.setQuery(queryBuilder)
    if (fields != null)
      searchRequestBuilder.setFetchSource(fields, null)
    iterateResults(searchRequestBuilder, consumer, scrollTimeout, scrollFetchSize, quitAfter)
  }

  @throws[ExecutionException]
  @throws[InterruptedException]
  def iterateResults(searchRequestBuilder: SearchRequestBuilder,
                     consumer: (EsMap) => Unit,
                     scrollTimeout: TimeValue = TimeValue.timeValueMinutes(2),
                     scrollFetchSize: Int = 1000,
                     quitAfter: Long = 0): Unit = {
    val searchResponse = searchRequestBuilder.setScroll(scrollTimeout)
                                             .setSize(scrollFetchSize)
                                             .execute
                                             .actionGet
    tailRecurse(searchResponse, consumer, if(quitAfter > 0) new AtomicLong(quitAfter) else null)
  }

  @tailrec
  private def tailRecurse(searchResponse: SearchResponse,
                          consumer: (EsMap) => Unit,
                          atomicLong: AtomicLong,
                          scrollTimeout: TimeValue = TimeValue.timeValueMinutes(2)): Unit = {
    val searchHits = searchResponse.getHits.getHits
    if(searchHits.length != 0) {
      for(sh <- searchHits
          if((atomicLong == null) || (atomicLong.decrementAndGet() > 0))) {
        consumer(new EsMap(sh))
      }
      tailRecurse(esClient.client
                          .prepareSearchScroll(searchResponse.getScrollId)
                          .setScroll(scrollTimeout)
                          .execute()
                          .actionGet,
                  consumer,
                  atomicLong,
                  scrollTimeout)
    }
  }
}
