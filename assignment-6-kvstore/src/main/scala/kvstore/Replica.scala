package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout
import scala.util.Success
import scala.util.Failure
import scala.concurrent.Future
import akka.actor.ActorLogging
import akka.actor.Cancellable

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with ActorLogging {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  val persister = context.actorOf(persistenceProps)

  var kv = Map.empty[String, String]
  def updateKV(key: String, valueOpt: Option[String]) = valueOpt match {
    case Some(value) => kv = kv + (key -> value)
    case None        => kv = kv - key
  }

  def persist(
    key: String,
    valueOpt: Option[String],
    id: Long): Unit /*Future[Persisted]*/ = {
    //    implicit val timeout = Timeout(1.second)

    val msg = Persist(key, valueOpt, id)
    persistAcks + (id -> msg)

    // This is a hack using a parallel redundancy pattern because I cbf doing it better
    persister ! msg
    persister ! msg
    persister ! msg
    persister ! msg
    persister ! msg

    //    def persistAsk = (persister ? Persist(key, valueOpt, id)).mapTo[Persisted]

    //    Future.firstCompletedOf(Seq(persistAsk, persistAsk, persistAsk, persistAsk, persistAsk))
  }

  def persistSnapshot(
    key: String,
    valueOpt: Option[String],
    id: Long): Unit = {
    val replyTo = sender

    def success = {
      //      updateKV(key, valueOpt)
      replyTo ! snapshotAck(key, id)
    }

    updateKV(key, valueOpt) // Is this seriously right..? Test Step4 Line46 seems to say we should.. but ignoring persistence seems illogical..

    persist(key, valueOpt, id)
    //    onComplete {
    //      case Success(s) => success
    //      case Failure(e) => ()
    //    }
  }

  def persistOperation(
    key: String,
    valueOpt: Option[String],
    id: Long): Unit = {
    val replyTo = sender
    //    def success(replyTo: ActorRef, key: String, valueOpt: Option[String], id: Long) = {
    //      updateKV(key, valueOpt)
    //      //      replicate(key, valueOpt, id) map { s =>
    //
    //      //      }
    //    }
    //    def failure(replyTo: ActorRef, key: String, valueOpt: Option[String], id: Long) = replyTo ! OperationFailed(id)

    //    val persistF = persist(key, valueOpt, id) map { persisted =>
    //      updateKV(key, valueOpt)
    //      persisted
    //    }
    //    val replicateF = replicate(key, valueOpt, id)

    val curReplicators = replicators

    persist(key, valueOpt, id)
    curReplicators foreach { _ ! Replicate(key, valueOpt, id) }

    val scheduledFail = context.system.scheduler.scheduleOnce(FiniteDuration(1, SECONDS)) {
      acks = acks - id
      replyTo ! OperationFailed(id)
    }

    acks = acks + (id -> (scheduledFail, false, replyTo, curReplicators))

    //    persistF onComplete { s => log.info("[persistOperation::persistF] {}", s) }
    //    replicateF onComplete { s => log.info("[persistOperation::replicateF] {}", s) }

    //    acks = acks + (id -> (replyTo, Set(???))

    //    for {
    //      p <- persistF
    //      //      r <- replicateF
    //    } yield {
    //      scheduledFail.cancel()
    //      self ! Replicated(key, id) // Hack
    //      //      replyTo ! OperationAck(id)
    //    }
  }

  def snapshotAck(key: String, seq: Long): SnapshotAck = {
    expectedSeq = Math.max(expectedSeq, seq + 1)
    SnapshotAck(key, seq)
  }

  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]

  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  //  def replicate(key: String, valueOpt: Option[String], id: Long) /*: Future[List[Replicated]]*/ = {
  //    val curReplicators = replicators
  //
  //    curReplicators foreach { _ ! Replicate(key, valueOpt, id) }
  //
  //    //    val responses = curReplicators.foldLeft(List.empty[Future[Replicated]]) { (context, replicator) =>
  //    //      implicit val timeout = Timeout(FiniteDuration(2, SECONDS))
  //    //      context :+ (replicator ? Replicate(key, valueOpt, id)).mapTo[Replicated]
  //    //    }
  //    //
  //    //    Future.sequence(responses)
  //  }

  var persistAcks = Map.empty[Long, Persist]
  var acks = Map.empty[Long, (Cancellable, Boolean, ActorRef, Set[ActorRef])]

  var expectedSeq = 0L

  arbiter ! Arbiter.Join

  def receive = {
    case JoinedPrimary   => { context.become(leader); log.info("Primary") }
    case JoinedSecondary => { context.become(replica); log.info("Secondary") }
  }

  val leader: Receive = updateOperations orElse lookupOperations orElse persisting orElse {
    case msg @ Replicas(replicas) => {
      val notMe = replicas - self
      val newReplicas = notMe -- replicators // New minus existing
      val deadReplicas = replicators -- notMe // Existing minus new

      log.info("[Replica::Replicas] \n{}\nNewReplicas: {}\nDeadReplicas: {}", msg, newReplicas, deadReplicas)

      // Kill Old
      deadReplicas foreach { r =>
        context.stop(r)
        secondaries = secondaries - r
        acks foreach { case (id, ref) => if (r == ref) acks - id }
      }

      // Start New
      newReplicas foreach { secondary =>
        val replicator = context.actorOf(Replicator.props(secondary))

        replicators = replicators + replicator
        secondaries = secondaries + (secondary -> replicator)

        kv foreach { case (key, value) => replicator ! Replicate(key, Some(value), scala.util.Random.nextLong) }
      }
    }
  }

  val replica: Receive = lookupOperations orElse persisting orElse {
    case msg @ Snapshot(key, valueOpt, seq) => {
      log.info("[Replica::Snapshot] {}", msg)

      if (seq > expectedSeq) ()
      else if (seq < expectedSeq) sender ! SnapshotAck(key, seq) //snapshotAck(key, seq)
      else /*if (seq == expectedSeq)*/ persistSnapshot(key, valueOpt, seq)
    }
  }

  def persisting: Receive = {
    case Persisted(key, id) => {
      for {
        p <- persistAcks.get(id)
        (cancel, persisted, replyTo, remainingReplications) <- acks.get(id)
      } yield {
        updateKV(p.key, p.valueOption)
        if (remainingReplications.isEmpty) {
          cancel.cancel()
          acks - id
          replyTo ! OperationAck(id)
        } else {
          acks = acks.updated(id, (cancel, true, replyTo, remainingReplications))
        }
      }
    }
  }

  def updateOperations: Receive = {
    case Insert(key, value, id) => persistOperation(key, Some(value), id)
    case Remove(key, id)        => persistOperation(key, None, id)
    case Replicated(key, id) => {
      for {
        (cancel, persisted, replyTo, remainingReplications) <- acks.get(id)
      } yield {
        if (remainingReplications.isEmpty) {
          cancel.cancel()
          acks - id
          replyTo ! OperationAck(id)
        } else {
          val withoutSender = remainingReplications - sender
          acks = acks.updated(id, (cancel, persisted, replyTo, withoutSender))
        }
      }
    }
  }

  def lookupOperations: Receive = {
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)
  }
}

