package com.broilogabriel

import java.net.InetSocketAddress

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.io.IO
import akka.io.Tcp
import akka.util.ByteString

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

  def receive = {
    case Received(data) => {
      println(data.decodeString(ByteString.UTF_8))
//      sender() ! Write(data)
      sender() ! Write(ByteString("response..."))
    }
    case PeerClosed => context stop self
  }
}
