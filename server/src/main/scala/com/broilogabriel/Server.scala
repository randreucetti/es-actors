package com.broilogabriel

import java.util.UUID

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.remote.Ack
import org.elasticsearch.action.bulk.BulkProcessor
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.transport.TransportClient

/**
  * Created by broilogabriel on 21/10/16.
  */
object Server {

  def mapArgs(args: Array[String]): Map[String, String] = {
    args.filter(_.startsWith("--")).map(_.split("=") match { case Array(k, v) => k.replaceFirst("--", "") -> v }).toMap
  }

  def main(args: Array[String]): Unit = {
    val margs = mapArgs(args)
    if (!margs.contains("cluster") || !margs.contains("host") || !margs.contains("port")) {
      System.exit(0)
    }

    val cluster = Cluster.getCluster(margs("cluster"), margs("host"), margs("port").toInt)

    val sys = ActorSystem.create("MigrationServer")
    val actor = sys.actorOf(Props(classOf[Server], cluster), name = "RemoteServer")
    actor ! "Starting Migration Server"
  }
}

class Server(cluster: TransportClient) extends Actor {

  def receive = {
    case Ack =>
      println("ACK received")
      val uuid = UUID.randomUUID
      val handler = context.actorOf(Props(classOf[BulkHandler], Cluster.getBulkProcessor(cluster).build()), name = uuid.toString)
      handler.forward(uuid)
    //      sender() ! 1

    case data: TransferObject => self.forward(data)

    case msg: String => println(s"Received: ${msg}")
  }

}

class BulkHandler(bulkProcessor: BulkProcessor) extends Actor {

  def receive = {

    case uuid: UUID =>
      println(s"It's me ${uuid.toString}")
      sender() ! uuid

    case data: TransferObject =>
      val indexRequest = new IndexRequest(data.index, data.hitType, data.hitId)
      indexRequest.source(data.source)
      bulkProcessor.add(indexRequest)

    case other => println(s"Something else here? $other")
  }

}
