package com.broilogabriel

import java.net.InetAddress
import java.util.UUID

import akka.actor._
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


/**
  * Created by broilogabriel on 24/10/16.
  */
object Cluster {

  def getCluster(cluster: Cluster): TransportClient = {
    val settings = Settings.settingsBuilder().put("cluster.name", cluster.name).build()
    TransportClient.builder().settings(settings).build()
      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(cluster.address), cluster.port))
  }

  def getBulkProcessor(listener: BulkListener): Builder = {
    BulkProcessor.builder(listener.client, listener)
      .setBulkActions(25000)
      .setBulkSize(new ByteSizeValue(25, ByteSizeUnit.MB))
      .setFlushInterval(TimeValue.timeValueSeconds(5))
  }

}

case class BulkListener(transportClient: TransportClient, handler: ActorRef) extends BulkProcessor.Listener with LazyLogging {

  def client: TransportClient = transportClient

  override def beforeBulk(executionId: Long, request: BulkRequest): Unit = {
    logger.info(s"${handler.path.name} Before: $executionId | Size: ${new ByteSizeValue(request.estimatedSizeInBytes()).getMb} " +
      s"| actions - ${request.numberOfActions()}")
  }

  override def afterBulk(executionId: Long, request: BulkRequest, response: BulkResponse): Unit = {
    logger.info(s"${handler.path.name} After: $executionId | Size: ${new ByteSizeValue(request.estimatedSizeInBytes()).getMb} " +
      s"| took - ${response.getTook}")
    handler ! request.numberOfActions()
  }

  override def afterBulk(executionId: Long, request: BulkRequest, failure: Throwable): Unit = {
    logger.info(s"${handler.path.name} ERROR $executionId done with failure: ${failure.getMessage}")
    handler ! request.numberOfActions()
  }

}

@SerialVersionUID(1000L)
case class Cluster(name: String, address: String, port: Int, totalHits: Long = 0)

@SerialVersionUID(2000L)
case class TransferObject(uuid: UUID, index: String, hitType: String, hitId: String, source: String)

@SerialVersionUID(1L)
object MORE extends Serializable

@SerialVersionUID(2L)
object DONE extends Serializable
