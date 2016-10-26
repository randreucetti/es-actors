package com.broilogabriel

import java.util.UUID

import org.elasticsearch.action.search.SearchType
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHit


/**
  * Created by broilogabriel on 24/10/16.
  */
object Cluster {

  def getCluster(cluster: Cluster): TransportClient = {
    val settings = ImmutableSettings.settingsBuilder().put("cluster.name", cluster.name).build()
    new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(cluster.address, cluster.port))
  }

  def checkIndex(cluster: TransportClient, index: String): Boolean = {
    cluster.admin().indices().prepareExists(index)
      .execute().actionGet().isExists
  }

  def getScrollId(cluster: TransportClient, index: String, size: Int = 5000) = {
    cluster.prepareSearch(index)
      .setSearchType(SearchType.SCAN)
      .setScroll(TimeValue.timeValueMinutes(5))
      .setQuery(QueryBuilders.matchAllQuery)
      .setSize(size)
      .execute().actionGet().getScrollId
  }

  def scroller(index: String, scrollId: String, cluster: TransportClient): Array[SearchHit] = {
    val partial = cluster.prepareSearchScroll(scrollId)
      .setScroll(TimeValue.timeValueMinutes(20))
      .execute()
      .actionGet()
    partial.getHits.hits()
  }

}

@SerialVersionUID(1000L)
case class Cluster(name: String, address: String, port: Int)

@SerialVersionUID(2000L)
case class TransferObject(uuid: UUID, index: String, hitType: String, hitId: String, source: String)

@SerialVersionUID(3000L)
object MORE

@SerialVersionUID(4000L)
object DONE