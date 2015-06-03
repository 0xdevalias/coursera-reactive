package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import scala.concurrent.duration._
import akka.actor.ActorLogging

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor with ActorLogging {
  import Replicator._
  import Replica._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  //  var pending = Vector.empty[Snapshot]

  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  // Resend unconfirmed
  context.system.scheduler.schedule(100.millis, 100.millis) {
    val toResend = acks
    toResend foreach { case (_, (_, Replicate(key, valueOpt, id))) => replica ! Snapshot(key, valueOpt, id) }
  }

  def receive: Receive = {
    case replicate @ Replicate(key, valueOpt, id) => {
      log.info("[Replicator::Replicate] {}", replicate)
      println(s"[Replicator::Replicate] $replicate")

      val seq = _seqCounter
      acks = acks + (seq -> (sender, replicate))

      replica ! Snapshot(key, valueOpt, seq)
      nextSeq
    }
    case msg @ SnapshotAck(key, seq) => {
      log.info("[Replicator::SnapshotAck] {}", msg)
      println(s"[Replicator::SnapshotAck] $msg")

      for {
        (replyTo, replicate) <- acks.get(seq)
        if (key == replicate.key)
      } yield {
        replyTo ! Replicated(key, replicate.id)
      }
    }
  }

}
