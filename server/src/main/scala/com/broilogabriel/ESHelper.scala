package com.broilogabriel

import java.net.InetAddress
import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.elasticsearch.action.bulk.BulkProcessor
import org.elasticsearch.action.bulk.BulkProcessor.Builder
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

  def getCluster(cluster: Cluster): TransportClient = {
    val settings = Settings.settingsBuilder().put("cluster.name", cluster.name).build()
    TransportClient.builder().settings(settings).build()
      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(cluster.address), cluster.port))
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

  def getBulkProcessor(listener: BulkListener): Builder = {
    BulkProcessor.builder(listener.client, listener)
      .setBulkActions(50000)
      .setBulkSize(new ByteSizeValue(25, ByteSizeUnit.MB))
      .setFlushInterval(TimeValue.timeValueSeconds(30))
  }

}

case class BulkListener(transportClient: TransportClient) extends BulkProcessor.Listener with LazyLogging {

  def client: TransportClient = transportClient

  override def beforeBulk(executionId: Long, request: BulkRequest): Unit = {
    logger.info(s"B: $executionId | ${new ByteSizeValue(request.estimatedSizeInBytes()).getMb} " +
      s"| actions - ${request.numberOfActions()}")
  }

  override def afterBulk(executionId: Long, request: BulkRequest, response: BulkResponse): Unit = {
    logger.info(s"A: $executionId | ${new ByteSizeValue(request.estimatedSizeInBytes()).getMb} " +
      s"| took - ${response.getTook}")
  }

  override def afterBulk(executionId: Long, request: BulkRequest, failure: Throwable): Unit = {
    logger.info(s"Bulk $executionId done with failure: ${failure.getMessage}")
  }

}

@SerialVersionUID(1000L)
case class Cluster(name: String, address: String, port: Int)

@SerialVersionUID(2000L)
case class TransferObject(uuid: UUID, index: String, hitType: String, hitId: String, source: String)

object MORE extends Serializable

object DONE extends Serializable
