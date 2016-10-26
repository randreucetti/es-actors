package com.broilogabriel

import java.util.UUID
import java.util.concurrent.TimeUnit._

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import org.elasticsearch.client.transport.TransportClient
import scopt.OptionParser

import scala.annotation.tailrec

/**
  * Created by broilogabriel on 24/10/16.
  */
case class Config(index: String = "",
  source: String = "localhost", sourcePort: Int = 9300, sourceCluster: String = "",
  target: String = "localhost", targetPort: Int = 9301, targetCluster: String = "",
  remoteAddress: String = "127.0.0.1", remotePort: Int = 9087, remoteName: String = "RemoteServer")

object Client {

  val name: String = com.broilogabriel.BuildInfo.name
  val version: String = com.broilogabriel.BuildInfo.version

  def parser: OptionParser[Config] = new OptionParser[Config]("es-client") {
    head(Client.name, Client.version)
    opt[String]('i', "index").required().valueName("<index>")
      .action((x, c) => c.copy(index = x))

    opt[String]('s', "source").valueName("<source_address>")
      .action((x, c) => c.copy(source = x)).text("default value 'localhost'")
    opt[Int]('p', "sourcePort").valueName("<source_port>")
      .action((x, c) => c.copy(sourcePort = x)).text("default value 9300")
    opt[String]('c', "sourceCluster").required().valueName("<source_cluster>")
      .action((x, c) => c.copy(sourceCluster = x))

    opt[String]('t', "target").valueName("<target_address>")
      .action((x, c) => c.copy(target = x)).text("default value 'localhost'")
    opt[Int]('r', "targetPort").valueName("<target_port>")
      .action((x, c) => c.copy(targetPort = x)).text("default value 9301")
    opt[String]('u', "targetCluster").required().valueName("<target_cluster>")
      .action((x, c) => c.copy(targetCluster = x))
  }

  def main(args: Array[String]): Unit = {
    parser.parse(args, Config()) match {
      case Some(config) => init(config)
      case None => println("Try again with the arguments")
    }
  }

  def init(config: Config): Unit = {
    val actorSystem = ActorSystem.create("MigrationClient")
    actorSystem.actorOf(Props(classOf[Client], config), "RemoteClient")
  }

}

class Client(config: Config) extends Actor {

  val cluster = Cluster.getCluster(Cluster(config.sourceCluster, config.source, config.sourcePort))
  val scrollId = Cluster.getScrollId(cluster, config.index)

  override def preStart(): Unit = {
    val path = s"akka.tcp://MigrationServer@${config.remoteAddress}:${config.remotePort}/user/${config.remoteName}"
    val remote = context.actorSelection(path)
    remote ! Cluster(config.targetCluster, config.target, config.targetPort)
  }

  override def postStop(): Unit = {
    println("Requested to stop. Will terminate the context.")
    context.system.terminate()
  }

  def receive = {

    case uuid: UUID =>
      println(s"Server is waiting to process $uuid")
      val totalSent: Int = sendWhile(System.currentTimeMillis(), cluster, config.index, scrollId, sender(), uuid)
      sender() ! totalSent
      println("Client done should wait for server.")
  }

  def formatElapsedTime(millis: Long): String = {
    val hours = MILLISECONDS.toHours(millis)
    val minutes = MILLISECONDS.toMinutes(millis)
    f"$hours%02d:${minutes - HOURS.toMinutes(hours)}%02d:${MILLISECONDS.toSeconds(millis) - MINUTES.toSeconds(minutes)}%02d"
  }

  @tailrec
  private def sendWhile(startTime: Long, cluster: TransportClient, index: String, scrollId: String, actor: ActorRef, uuid: UUID, total: Int = 0): Int = {
    val hits = Cluster.scroller(index, scrollId, cluster)
    if (hits.nonEmpty) {
      hits.foreach(hit => {
        val data = TransferObject(uuid, index, hit.getType, hit.getId, hit.getSourceAsString)
        actor ! data
      })
      val sent = hits.length + total
      println(s"Sent $sent in ${formatElapsedTime(System.currentTimeMillis() - startTime)}")
      sendWhile(startTime, cluster, index, scrollId, actor, uuid, sent)
    } else {
      total
    }
  }

}