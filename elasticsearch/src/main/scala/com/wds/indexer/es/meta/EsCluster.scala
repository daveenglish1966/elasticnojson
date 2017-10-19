package com.wds.indexer.es.meta

class EsCluster(val clusterName: String, val hostPorts : List[HostPort]) {
  override def toString: String = clusterName.concat(": ").concat(hostPorts.mkString(","))
}
