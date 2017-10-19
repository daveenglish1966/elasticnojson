package com.wds.indexer.es.meta

class HostPort(val host: String, val port: Int) {
  override def toString: String = s"$host:$port"
}
