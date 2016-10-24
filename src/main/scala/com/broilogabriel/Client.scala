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

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Promise

/**
  * Created by broilogabriel on 24/10/16.
  */
object Client {

  def main(args: Array[String]): Unit = {
    val promise = Promise[String]()
    val props = Props(
      classOf[Client],
      new InetSocketAddress("localhost", 9021),
      s"GET / HTTP/1.1\r\nHost: localhost\r\nAccept: */*\r\n\r\n",
      promise)
    val sys = ActorSystem.create("MyActorSystem")
    val actor = sys.actorOf(props)
    promise.future map { data =>
      actor ! "close"
      println(data)
    }
  }

}

class Client(remote: InetSocketAddress, requestData: String, thePromise: Promise[String]) extends Actor {

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
      connection ! Write(ByteString(requestData))

      context become {
        case CommandFailed(w: Write) =>
          println("Failed to write request.")
        case Received(data) =>
          println("Received response.")
          thePromise.success(data.decodeString("UTF-8"))
        case "close" =>
          println("Closing connection")
          connection ! Close
        case _: ConnectionClosed =>
          println("Connection closed by server.")
          context stop self
      }
    case _ => println("Something else is up.")
  }
}