package com.broilogabriel

import java.net.InetAddress

import akka.actor.ActorRef
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

object Cluster {

  def getCluster(cluster: ClusterConfig): TransportClient = {
    val settings = Settings.settingsBuilder().put("cluster.name", cluster.name).build()
    TransportClient.builder().settings(settings).build()
      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(cluster.address), cluster.port))
  }

  def getBulkProcessor(listener: BulkListener): Builder = {
    BulkProcessor.builder(listener.client, listener)
      .setBulkActions(ClusterConfig.bulkActions)
      .setBulkSize(new ByteSizeValue(ClusterConfig.bulkSizeMb, ByteSizeUnit.MB))
      .setFlushInterval(TimeValue.timeValueSeconds(ClusterConfig.flushIntervalSec))
  }

}

case class BulkListener(
  transportClient: TransportClient, handler: ActorRef
) extends BulkProcessor.Listener with LazyLogging {

  def client: TransportClient = transportClient

  override def beforeBulk(executionId: Long, request: BulkRequest): Unit = {
    logger.info(s"${handler.path.name} Before: $executionId | Size: " +
      s"${new ByteSizeValue(request.estimatedSizeInBytes()).getMb} " +
      s"| actions - ${request.numberOfActions()}")
  }

  override def afterBulk(executionId: Long, request: BulkRequest, response: BulkResponse): Unit = {
    logger.info(s"${handler.path.name} After: $executionId | Size: " +
      s"${new ByteSizeValue(request.estimatedSizeInBytes()).getMb} " +
      s"| took - ${response.getTook}")
    handler ! request.numberOfActions()
  }

  override def afterBulk(executionId: Long, request: BulkRequest, failure: Throwable): Unit = {
    logger.info(s"${handler.path.name} ERROR $executionId done with failure: ${failure.getMessage}")
    handler ! request.numberOfActions()
  }

}