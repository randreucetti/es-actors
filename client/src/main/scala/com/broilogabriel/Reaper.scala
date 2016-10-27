package com.broilogabriel

/**
  * Created by ross on 26/10/16.
  */

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Terminated
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable.ArrayBuffer

object Reaper {

  // Used by others to register an Actor for watching
  case class WatchMe(ref: ActorRef)

}

abstract class Reaper extends Actor {

  import Reaper._

  // Keep track of what we're watching
  val watched = ArrayBuffer.empty[ActorRef]

  // Derivations need to implement this method.  It's the
  // hook that's called when everything's dead
  def allSoulsReaped(): Unit

  // Watch and check for termination
  final def receive = {
    case WatchMe(ref) =>
      context.watch(ref)
      watched += ref
    case Terminated(ref) =>
      watched -= ref
      if (watched.isEmpty) allSoulsReaped()
  }
}

class ProductionReaper extends Reaper with LazyLogging {
  // Shutdown
  def allSoulsReaped(): Unit = {
    logger.info("all done, reaping souls")
    context.system.terminate()
  }
}
