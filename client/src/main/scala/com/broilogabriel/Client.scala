package com.broilogabriel

import java.util.UUID

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.remote.Ack
import org.elasticsearch.client.transport.TransportClient

import scala.annotation.tailrec

/**
  * Created by broilogabriel on 24/10/16.
  */
object Client {

  def mapArgs(args: Array[String]): Map[String, String] = {
    args.filter(_.startsWith("--")).map(_.split("=") match { case Array(k, v) => k.replaceFirst("--", "") -> v }).toMap
  }

  def main(args: Array[String]): Unit = {

    val margs = mapArgs(args)
    if (!margs.contains("index") || !margs.contains("cluster") || !margs.contains("host") || !margs.contains("port")) {
      System.exit(0)
    }

    val cluster = Cluster.getCluster(margs("cluster"), margs("host"), margs("port").toInt)
    val index = margs("index")
    val scrollId = Cluster.getScrollId(cluster, index)

    val actorSystem = ActorSystem.create("MigrationClient")

    val actor = actorSystem.actorOf(Props(classOf[Client], cluster, index, scrollId), "RemoteClient")
    actor ! "Let this shit begin"

  }

}

class Client(cluster: TransportClient, index: String, scrollId: String) extends Actor {

  override def preStart(): Unit = {
    val remote = context.actorSelection("akka.tcp://MigrationServer@127.0.0.1:9087/user/RemoteServer")
    remote ! Ack
  }

  def receive = {

    case uuid: UUID =>
      println(uuid)
      self ! sendWhile(cluster, index, scrollId, sender(), uuid)

    case some: Int =>
      //      context.stop(self)
      //      context.system.terminate()
      println("Client done should wait for server.")
  }

  @tailrec
  private def sendWhile(cluster: TransportClient, index: String, scrollId: String, actor: ActorRef, uuid: UUID, total: Int = 0): Int = {
    val hits = Cluster.scroller(index, scrollId, cluster)
    if (hits.nonEmpty) {
      hits.foreach(hit => {
        val data = TransferObject(uuid, index, hit.getType, hit.getId, hit.getSourceAsString)
        actor ! data
      })
      val sent = hits.size + total
      println(s"Total sent: $sent")
      sendWhile(cluster, index, scrollId, actor, uuid, sent)
    } else {
      total
    }
  }

}