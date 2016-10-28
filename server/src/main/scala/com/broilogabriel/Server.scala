package com.broilogabriel

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import com.typesafe.scalalogging.LazyLogging
import org.elasticsearch.action.index.IndexRequest

/**
  * Created by broilogabriel on 21/10/16.
  */
object Server extends App with LazyLogging {
  logger.info(s"${BuildInfo.name} - ${BuildInfo.version}")
  val actorSystem = ActorSystem.create("MigrationServer")
  val actor = actorSystem.actorOf(Props[Server], name = "RemoteServer")
}

class Server extends Actor with LazyLogging {

  def receive = {

    case cluster: Cluster =>
      val uuid = UUID.randomUUID
      logger.info(s"Received cluster config: $cluster")
      context.actorOf(
        Props(classOf[BulkHandler], cluster, sender),
        name = uuid.toString
      ).forward(uuid)

    case other => logger.info(s"Unknown message: $other")

  }

}

class BulkHandler(cluster: Cluster, origin: ActorRef) extends Actor with LazyLogging {

  val bListener = BulkListener(Cluster.getCluster(cluster), origin)
  val bulkProcessor = Cluster.getBulkProcessor(bListener).build()
  val finishedActions: AtomicLong = new AtomicLong

  override def postStop(): Unit = {
    logger.info(s"Stopping BulkHandler ${self.path.name}")
    bulkProcessor.flush()
    bListener.client.close()
  }

  override def receive = {

    case uuid: UUID =>
      logger.info(s"It's me ${uuid.toString}")
      sender ! uuid

    case to: TransferObject =>
      val indexRequest = new IndexRequest(to.index, to.hitType, to.hitId)
      indexRequest.source(to.source)
      bulkProcessor.add(indexRequest)

    case DONE =>
      logger.info("Received DONE, gonna send PoisonPill")
      sender ! PoisonPill

    case finished: Int =>
      val actions = finishedActions.addAndGet(finished)
      logger.info(s"Processed $actions of ${cluster.totalHits}")
      if (actions < cluster.totalHits) {
        sender ! MORE
      } else {
        self ! PoisonPill
      }

    case other => logger.info(s"Something else here? $other")
  }

}
