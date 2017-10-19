package com.wds.indexer.es

import java.net.InetAddress

import com.wds.indexer.es.meta.EsCluster
import org.apache.commons.lang3.StringUtils
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient

/**
  * You only need ONE OF THESE PER JVM.
  *
  * This class is used to instantiate all of the other EsClient* classes.
  *
  * @param esCluster
  */
class EsClient(val esCluster: EsCluster,
               val userName: String = "",
               private val password: String = "") {
  private val settingsBuilder = Settings.builder
                                        .put("cluster.name", esCluster.clusterName)
  private val userSecurity = (!StringUtils.isEmpty(userName)) && (!StringUtils.isEmpty(password))
  if(userSecurity) {
    settingsBuilder.put("xpack.security.user", s"$userName:$password")
  }
  private val settings = settingsBuilder.build
  private val transportClient: TransportClient = if(userSecurity)
    new PreBuiltXPackTransportClient(settings)
  else
    new PreBuiltTransportClient(settings)
  esCluster.hostPorts.foreach(hp =>
    transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hp.host),
                                                                       hp.port)))
  val client: Client = transportClient

  override def toString: String = s"${esCluster.toString}: userSecurity=$userSecurity${if(userSecurity) userName else ""}"
}