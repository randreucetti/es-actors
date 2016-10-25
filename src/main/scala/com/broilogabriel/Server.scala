package com.broilogabriel

import java.net.InetSocketAddress

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.io.IO
import akka.io.Inet.SO.ReceiveBufferSize
import akka.io.Inet.SO.SendBufferSize
import akka.io.Tcp
import akka.util.ByteString
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
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
    val actor = sys.actorOf(Props(classOf[Server], cluster))
  }
}

class Server(cluster: TransportClient) extends Actor {

  import Tcp._
  import context.system

  IO(Tcp) ! Bind(self, new InetSocketAddress("localhost", 9021), options = List(SendBufferSize(Integer.MAX_VALUE), ReceiveBufferSize(Integer.MAX_VALUE)))

  def receive = {
    case b@Bound(localAddress) => println(s"Bounded to ${localAddress.getHostName}:${localAddress.getPort}")

    case CommandFailed(_: Bind) => context stop self

    case c@Connected(remote, local) =>
      println(s"new connected? ${remote.getHostName}")
      val bulkProcessor = Cluster.getBulkProcessor(cluster).build()
      val handler = context.actorOf(Props(classOf[SimplisticHandler], bulkProcessor))
      val connection = sender()
      connection ! Register(handler)

    case _ => println("Something else here?")
  }

}

class SimplisticHandler(bulkProcessor: BulkProcessor) extends Actor {

  import Tcp._

  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

  def receive = {
    case Received(data) => {
      val str = data.decodeString(ByteString.UTF_8)
      if ("Ok?" == str) {
        sender() ! Write(ByteString("Ok"))
      } else {
        try {
          val decoded = mapper.readValue[TransferObject](str)
          val indexRequest = new IndexRequest(decoded.index, decoded.hitType, decoded.hitId)
          indexRequest.source(decoded.source)
          bulkProcessor.add(indexRequest)
        } catch {
          case e: Exception => println(s"${e.getClass} | ${str.length}")
        }
      }
    }
    case PeerClosed => {
      println(s"Client disconnected")
      context stop self
    }
    case other => println(s"Something else here? $other")
  }
}
