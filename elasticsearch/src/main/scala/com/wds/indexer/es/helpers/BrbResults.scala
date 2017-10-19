package com.wds.indexer.es.helpers

import org.elasticsearch.action.bulk.BulkResponse

import scala.collection.mutable.ListBuffer

class BrbResults(val bulkResponse: BulkResponse) {
  private val listBuffer = new ListBuffer[BrbResult]
  if (bulkResponse.hasFailures) {
    for (bir <- bulkResponse.getItems) {
      if (bir.isFailed) {
        listBuffer += BrbResult(true, bir.getFailureMessage, null)
      } else {
        listBuffer += BrbResult(false, null, bir.getId)
      }
    }
  } else {
    for (bir <- bulkResponse.getItems) {
      listBuffer += BrbResult(false, null, bir.getId)
    }
  }
  val responses = listBuffer.toList

  @inline def hasFailures(): Boolean = bulkResponse.hasFailures

  def errors(title: String) : String = {
    val errors = new ListBuffer[String]
    for (bir <- bulkResponse.getItems)
      if (bir.isFailed)
        errors += bir.getFailureMessage
    title + ":\n" + "  " + errors.toList.mkString("\n  ")
  }
}
