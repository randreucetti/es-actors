package com.broilogabriel

import java.net.InetSocketAddress

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.io.IO
import akka.io.Tcp
import akka.util.ByteString
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

/**
  * Created by broilogabriel on 21/10/16.
  */
object Server {
  def main(args: Array[String]): Unit = {
    val props = Props(classOf[Server])
    val sys = ActorSystem.create("MyActorSystem")
    val actor = sys.actorOf(props)
  }
}

class Server extends Actor {

  import Tcp._
  import context.system

  IO(Tcp) ! Bind(self, new InetSocketAddress("localhost", 9021))

  def receive = {
    case b@Bound(localAddress) => println("BOUNDED")
    // do some logging or setup ...

    case CommandFailed(_: Bind) => context stop self

    case c@Connected(remote, local) =>
      val handler = context.actorOf(Props[SimplisticHandler])
      val connection = sender()
      connection ! Register(handler)
  }

}

class SimplisticHandler extends Actor {

  import Tcp._

  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)


  def receive = {
    case Received(data) => {
      val str = data.decodeString(ByteString.UTF_8)
      if ("Ok?" == str) {
        sender() ! Write(ByteString("Ok"))
      } else {
        println(str)
        val decoded = mapper.readValue[Seq[Map[String, String]]](str)
      }
    }
    case PeerClosed => {
      println(s"Client disconnected")
      context stop self
    }
    case other => println(s"Something else here? $other")
  }
}
