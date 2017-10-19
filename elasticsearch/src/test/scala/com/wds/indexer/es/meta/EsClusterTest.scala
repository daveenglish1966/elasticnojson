package com.wds.indexer.es.meta

import org.scalatest.FunSuite

class EsClusterTest extends FunSuite {

  test("test esCluster.toString") {
    val esCluster: EsCluster = new EsCluster("es-docker",
                                             List(new HostPort("10.127.06.20", 8080),
                                                  new HostPort("10.127.06.21", 8080)))
    println(esCluster);
  }
}
