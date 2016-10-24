package com.broilogabriel

import java.net.InetSocketAddress

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.io.IO
import akka.io.Tcp
import akka.io.Tcp.Close
import akka.io.Tcp.CommandFailed
import akka.io.Tcp.Connect
import akka.io.Tcp.Connected
import akka.io.Tcp.ConnectionClosed
import akka.io.Tcp.Received
import akka.io.Tcp.Register
import akka.io.Tcp.Write
import akka.util.ByteString
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.elasticsearch.client.transport.TransportClient

import scala.annotation.tailrec
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
    val props = Props(
      classOf[Client],
      new InetSocketAddress("localhost", 9021),
      promise
    )
    val sys = ActorSystem.create("MyActorSystem")

    sendWhile(cluster, index, scrollId, props, sys)

    //    val actor = sys.actorOf(props)
    //    promise.future.map { data =>
    //      actor ! "close"
    //      actor ! Write(ByteString("close"))
    //    }

  }

  @tailrec
  private def sendWhile(
    cluster: TransportClient,
    index: String,
    scrollId: String,
    props: Props,
    actorSystem: ActorSystem,
    total: Int = 0): Int = {

    val ret = Cluster.scroller(index, scrollId, cluster)
    if (ret.nonEmpty) {
      val actor = actorSystem.actorOf(props)
      val str = mapper.writeValueAsString(ret)
      actor ! Write(ByteString(str))
      val sent = ret.size + total
      println(s"Total sent: $sent")
      sendWhile(cluster, index, scrollId, props, actorSystem, sent)
    } else {
      total
    }
  }

}

class Client(
  remote: InetSocketAddress,
  thePromise: Promise[Int]) extends Actor {

  import context.system

  println("Connecting")
  IO(Tcp) ! Connect(remote)

  def receive = {
    case CommandFailed(_: Connect) =>
      println("Connect failed")
      context stop self

    case c@Connected(remote, local) =>
      println("Connect succeeded")
      val connection = sender()
      connection ! Register(self)
      println("Sending request early")
      connection ! Write(ByteString("Ok?"))
      context become {
        case CommandFailed(w: Write) =>
          println("Failed to write request.")
        case Received(data) =>
          val received = data.decodeString(ByteString.UTF_8)
          println(s"Received response. ${received}")
          if ("Ok" == received) {
            context stop self
            System.exit(0)
          }
        case "close" =>
          println("Closing connection")
        case _: ConnectionClosed =>
          println("Connection closed by server.")
          context stop self
        case Close => println("Should close now 2")
      }

    case Received(data) => println(s"Something else is up. ${data.decodeString(ByteString.UTF_8)}")

    case Close => println("Should close now 1")
  }
}

//class Handler extends Actor {
//
//  import Tcp._
//
//  val mapper = new ObjectMapper() with ScalaObjectMapper
//  mapper.registerModule(DefaultScalaModule)
//
//
//  def receive = {
//    case CommandFailed(w: Write) =>
//      println("Failed to write request.")
//    case Received(data) =>
//      val received = data.decodeString(ByteString.UTF_8)
//      println(s"Received response. ${received}")
//      if ("Ok" == received) {
//        //        thePromise.success(sendWhile(connection))
//        //        context stop self
//        //        System.exit(0)
//      }
//    case "close" =>
//      println("Closing connection")
//    case _: ConnectionClosed =>
//      println("Connection closed by server.")
//      context stop self
//    case Close => println("Should close now 2")
//  }
//}