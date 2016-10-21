package com.broilogabriel

import akka.actor.ActorSystem
import akka.actor.Props

/**
  * Created by broilogabriel on 21/10/16.
  */
object Main {

  def main(args: Array[String]): Unit = {
    val props = Props(classOf[Server])
    val sys = ActorSystem.create("MyActorSystem")
    val tcpActor = sys.actorOf(props)
  }

  //  val system: ActorSystem = ActorSystem("socketserver")
  //
  //  val port = Option(System.getenv("SOCKETPORT")).map(_.toInt).getOrElse(0)
  //  val addressPromise = Promise[SocketAddress]()(system.dispatcher)
  //  val server = system.actorOf(Props(new SocketServer(new InetSocketAddress("localhost", port), addressPromise)))
  //  addressPromise.map(address => println("Started Socket Server, listening on:" + address))
  //
  //  ShutdownHookThread {
  //    println("Socket Server exiting...")
  //    system.shutdown()
  //    system.awaitTermination()
  //  }

}
