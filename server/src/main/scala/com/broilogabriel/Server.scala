package com.broilogabriel

import java.util.UUID

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import org.elasticsearch.action.bulk.BulkProcessor
import org.elasticsearch.action.index.IndexRequest

/**
  * Created by broilogabriel on 21/10/16.
  */
object Server extends App {
  val actorSystem = ActorSystem.create("MigrationServer")
  val actor = actorSystem.actorOf(Props[Server], name = "RemoteServer")
  actor ! "Starting Migration Server"
}

class Server extends Actor {

  def receive = {

    case cluster: Cluster =>
      val uuid = UUID.randomUUID
      println(s"Received cluster config: $cluster")
      val handler = context.actorOf(Props(classOf[BulkHandler], Cluster.getBulkProcessor(Cluster.getCluster(cluster))
        .build()), name = uuid.toString)
      handler.forward(uuid)

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

    case DONE =>
      sender() ! MORE

    case some: Int =>
      println(s"Client sent $some, sending PoisonPill now")
      sender() ! PoisonPill

    case other => println(s"Something else here? $other")
  }

}
