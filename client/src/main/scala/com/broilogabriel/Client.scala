package com.broilogabriel

import java.util.UUID
import java.util.concurrent.TimeUnit._

import akka.actor._
import com.broilogabriel.Reaper.WatchMe
import com.typesafe.scalalogging.LazyLogging
import org.elasticsearch.client.transport.TransportClient
import org.joda.time.DateTime
import org.joda.time.DateTimeConstants
import scopt.OptionParser

import scala.annotation.tailrec

/**
  * Created by broilogabriel on 24/10/16.
  */
case class Config(index: String = "", indices: Set[String] = Set.empty,
  sourceAddress: String = "localhost", sourcePort: Int = 9300, sourceCluster: String = "",
  targetAddress: String = "localhost", targetPort: Int = 9301, targetCluster: String = "",
  remoteAddress: String = "127.0.0.1", remotePort: Int = 9087, remoteName: String = "RemoteServer") {

  def source: Cluster = Cluster(name = sourceCluster, address = sourceAddress, port = sourcePort)

  def target: Cluster = Cluster(name = targetCluster, address = targetAddress, port = targetPort)
}

object Client extends LazyLogging {

  def formatElapsedTime(millis: Long): String = {
    val hours = MILLISECONDS.toHours(millis)
    val minutes = MILLISECONDS.toMinutes(millis)
    f"$hours%02d:${minutes - HOURS.toMinutes(hours)}%02d:${MILLISECONDS.toSeconds(millis) - MINUTES.toSeconds(minutes)}%02d"
  }

  def indicesByRange(startDate: String, endDate: String, validate: Boolean = false): Option[Set[String]] = {
    try {
      val sd = DateTime.parse(startDate).withDayOfWeek(DateTimeConstants.SUNDAY)
      logger.info(s"Start date: $sd")
      val ed = DateTime.parse(endDate).withDayOfWeek(DateTimeConstants.SUNDAY)
      logger.info(s"End date: $ed")
      if (sd.getMillis < ed.getMillis) {
        Some(if (!validate) getIndices(sd, ed) else Set.empty)
      } else {
        None
      }
    } catch {
      case e: IllegalArgumentException => None
    }
  }

  @tailrec
  def getIndices(startDate: DateTime, endDate: DateTime, indices: Set[String] = Set.empty): Set[String] = {
    if (startDate.getMillis > endDate.getMillis) {
      indices
    } else {
      getIndices(startDate.plusWeeks(1), endDate, indices + s"a-${startDate.getWeekyear}-${startDate.getWeekOfWeekyear}")
    }
  }

  def parser: OptionParser[Config] = new OptionParser[Config]("es-client") {
    head(BuildInfo.name, BuildInfo.version)

    opt[Seq[String]]('i', "indices").valueName("<index1>,<index2>...")
      .action((x, c) => c.copy(indices = x.toSet))
    opt[(String, String)]('d', "dateRange").validate(
      d => if (indicesByRange(d._1, d._2, validate = true).isDefined) success else failure("Invalid dates")
    ).action({
      case ((start, end), c) => c.copy(indices = indicesByRange(start, end).get)
    }).keyValueName("<start_date>", "<end_date>").text("Start date value should be lower than end date.")

    opt[String]('s', "source").valueName("<source_address>")
      .action((x, c) => c.copy(sourceAddress = x)).text("default value 'localhost'")
    opt[Int]('p', "sourcePort").valueName("<source_port>")
      .action((x, c) => c.copy(sourcePort = x)).text("default value 9300")
    opt[String]('c', "sourceCluster").required().valueName("<source_cluster>")
      .action((x, c) => c.copy(sourceCluster = x))

    opt[String]('t', "target").valueName("<target_address>")
      .action((x, c) => c.copy(targetAddress = x)).text("default value 'localhost'")
    opt[Int]('r', "targetPort").valueName("<target_port>")
      .action((x, c) => c.copy(targetPort = x)).text("default value 9301")
    opt[String]('u', "targetCluster").required().valueName("<target_cluster>")
      .action((x, c) => c.copy(targetCluster = x))

    opt[String]("remoteAddress").valueName("<remote_address>").action((x, c) => c.copy(remoteAddress = x))
    opt[Int]("remotePort").valueName("<remote_port>").action((x, c) => c.copy(remotePort = x))
    opt[String]("remoteName").valueName("<remote_name>").action((x, c) => c.copy(remoteName = x))

    help("help").text("Prints the usage text.")
  }

  def main(args: Array[String]): Unit = {
    parser.parse(args, Config()) match {
      case Some(config) => if (config.indices.nonEmpty) {
        init(config)
      } else {
        logger.info("Missing indices. Check help to send index")
      }
      case None => logger.info("Try again with the arguments")
    }
  }

  def init(config: Config): Unit = {
    val actorSystem = ActorSystem.create("MigrationClient")
    val reaper = actorSystem.actorOf(Props(classOf[ProductionReaper]))
    logger.info(s"Creating actors for indices ${config.indices}")
    config.indices.foreach(index => {
      val actorRef = actorSystem.actorOf(Props(classOf[Client], config.copy(index = index, indices = Set.empty)), s"RemoteClient-$index")
      reaper ! WatchMe(actorRef)
    }
    )
  }

}

class Client(config: Config) extends Actor with LazyLogging {
  var scrollId: String = ""
  var cluster: TransportClient = _
  var uuid: UUID = _

  override def preStart(): Unit = {
    val path = s"akka.tcp://MigrationServer@${config.remoteAddress}:${config.remotePort}/user/${config.remoteName}"
    val remote = context.actorSelection(path)
    remote ! config.target
  }

  override def postStop(): Unit = {
    logger.info("Requested to stop.")
  }

  def receive = {

    case MORE =>
      val finished = sendWhile(System.currentTimeMillis(), cluster, config.index, scrollId, sender(), uuid)
      if (finished) {
        sender() ! 1
      }

    case uuidInc: UUID =>
      uuid = uuidInc
      logger.info(s"Server is waiting to process $uuid")
      cluster = Cluster.getCluster(config.source)
      if (Cluster.checkIndex(cluster, config.index)) {
        scrollId = Cluster.getScrollId(cluster, config.index)
        val finished = sendWhile(System.currentTimeMillis(), cluster, config.index, scrollId, sender(), uuid)
        if (finished) {
          sender() ! 1
        }
      } else {
        logger.info(s"Invalid index ${config.index}")
        sender() ! s"Invalid index ${config.index}"
        self ! PoisonPill
      }

  }

  private def sendWhile(startTime: Long, cluster: TransportClient, index: String, scrollId: String, actor: ActorRef, uuid: UUID, total: Int = 0): Boolean = {
    val hits = Cluster.scroller(index, scrollId, cluster)
    if (hits.nonEmpty) {
      hits.foreach(hit => {
        val data = TransferObject(uuid, index, hit.getType, hit.getId, hit.getSourceAsString)
        actor ! data
      })
      val sent = hits.length + total
      logger.info(s"Sent $sent in ${Client.formatElapsedTime(System.currentTimeMillis() - startTime)}")
      actor ! DONE
      false
    } else {
      true
    }
  }

}