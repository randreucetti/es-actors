package com.broilogabriel

import java.net.InetAddress

import org.elasticsearch.action.bulk.BulkProcessor
import org.elasticsearch.action.bulk.BulkProcessor.Builder
import org.elasticsearch.action.bulk.BulkProcessor.Listener
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.unit.ByteSizeUnit
import org.elasticsearch.common.unit.ByteSizeValue
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHit


/**
  * Created by broilogabriel on 24/10/16.
  */
object Cluster {

  def getCluster(clusterName: String, address: String, port: Int): TransportClient = {
    val settings = Settings.settingsBuilder().put("cluster.name", clusterName).build()
    TransportClient.builder().settings(settings).build()
      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(address), port))
  }

  def getBulkProcessor(cluster: TransportClient): Builder = {
    BulkProcessor.builder(cluster, new Listener {
      override def beforeBulk(executionId: Long, request: BulkRequest): Unit =
        println(s"Before $executionId | actions - ${request.numberOfActions()}")

      override def afterBulk(executionId: Long, request: BulkRequest, response: BulkResponse): Unit =
        println(s"Bulk $executionId done in ${response.getTook}")

      override def afterBulk(executionId: Long, request: BulkRequest, failure: Throwable): Unit =
        println(s"Bulk $executionId done with failure: ${failure.getMessage}")

    }).setBulkActions(50000).setBulkSize(new ByteSizeValue(25, ByteSizeUnit.MB))
  }

  def getScrollId(cluster: TransportClient, index: String, size: Int = 5000) = {
    cluster.prepareSearch(index)
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

case class TransferObject(index: String, hitType: String, hitId: String, source: String)
