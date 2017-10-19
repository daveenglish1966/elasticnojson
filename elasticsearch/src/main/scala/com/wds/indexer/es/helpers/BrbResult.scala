package com.wds.indexer.es.helpers

case class BrbResult(val hasError: Boolean,
                     val errorMessage: String,
                     val id: String)