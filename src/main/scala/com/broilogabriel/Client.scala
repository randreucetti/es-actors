package com.broilogabriel

import java.net.InetSocketAddress

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.io.IO
import akka.io.Tcp
import akka.io.Tcp.CommandFailed
import akka.io.Tcp.Connect
import akka.io.Tcp.Connected
import akka.io.Tcp.Register
import akka.io.Tcp.Write
import akka.util.ByteString
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.elasticsearch.client.transport.TransportClient

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Promise

/**
  * Created by broilogabriel on 24/10/16.
  */
object Client {

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

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

    val promise = Promise[Int]()
    val actorSystem = ActorSystem.create("MigrationClient")

    val handler = actorSystem.actorOf(Props(classOf[Handler], cluster, index, scrollId, promise), "Handler")

    val actor = actorSystem.actorOf(Props(classOf[Client], new InetSocketAddress("localhost", 9021), handler), "Connection")

    promise.future.map { data =>
      println(s"Done: $data")
      actor ! data
    }

  }

}

class Client(remote: InetSocketAddress, handler: ActorRef) extends Actor {

  import context.system

  println("Connecting")
  IO(Tcp) ! Connect(remote)

  def receive = {
    case CommandFailed(_: Connect) =>
      println("Connect failed")
      context stop self

    case c@Connected(remote, local) =>

      //      val handler = context.actorOf(Props(classOf[Handler], promise))
      val connection = sender()
      connection ! Register(handler)
      connection ! Write(ByteString("Ok?"))

    case some: Int =>
      println(s"Received $some to force finish")
      context.stop(self)
      context.system.terminate()
      println("Terminated")
  }
}

class Handler(cluster: TransportClient, index: String, scrollId: String, promise: Promise[Int]) extends Actor {

  import Tcp._

  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

  @tailrec
  private def sendWhile(cluster: TransportClient, index: String, scrollId: String, actor: ActorRef, total: Int = 0): Int = {
    val hits = Cluster.scroller(index, scrollId, cluster)
    if (hits.nonEmpty) {
      hits.foreach(hit => {
        // TODO huge data will cause problems because tcp limitations, CompoundWrite may be a solution
        val data = ByteString(mapper.writeValueAsString(TransferObject(index, hit.getType, hit.getId, hit.getSourceAsString)))
        Thread.sleep(1)
        actor ! Write(data)
      })
      val sent = hits.size + total
      println(s"Total sent: $sent")
      sendWhile(cluster, index, scrollId, actor, sent)
    } else {
      total
    }
  }

  def receive = {
    case CommandFailed(w: Write) =>
      println("Failed to write request.")
    case Received(data) =>
      val received = data.decodeString(ByteString.UTF_8)
      println(s"Received response. ${received}")
      received match {
        case "Ok" => {
          promise.success(sendWhile(cluster, index, scrollId, sender()))
        }
        case other => println(other)
      }
    case "close" =>
      println("Closing connection")
    case _: ConnectionClosed =>
      println("Connection closed by server.")
      context stop self
    case Close => println("Should close now 2")
  }
}