/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /**
   * Request with identifier `id` to insert an element `elem` into the tree.
   * The actor at reference `requester` should be notified when this operation
   * is completed.
   */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /**
   * Request with identifier `id` to check whether an element `elem` is present
   * in the tree. The actor at reference `requester` should be notified when
   * this operation is completed.
   */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /**
   * Request with identifier `id` to remove the element `elem` from the tree.
   * The actor at reference `requester` should be notified when this operation
   * is completed.
   */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /**
   * Holds the answer to the Contains request with identifier `id`.
   * `result` is true if and only if the element is present in the tree.
   */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}

class BinaryTreeSet extends Actor with Stash {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  //  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case op: Operation => root ! op
    case GC => {
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))
    }
  }

  // optional
  /**
   * Handles messages while garbage collection is performed.
   * `newRoot` is the root of the new binary tree where we want to copy
   * all non-removed elements into.
   */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case op: Operation => stash()
    case CopyFinished => {
      root ! PoisonPill
      root = newRoot
      context.become(normal)
      unstashAll()
    }
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    // Forwards
    case op: Operation if (op.elem < elem && subtrees.isDefinedAt(Left))  => subtrees(Left) ! op
    case op: Operation if (op.elem > elem && subtrees.isDefinedAt(Right)) => subtrees(Right) ! op

    case Insert(requester, id, el) => {
      if (el == elem) removed = false
      else if (el < elem) insert(el, Left)
      else if (el > elem) insert(el, Right)

      requester ! OperationFinished(id)
    }
    case c @ Contains(requester, id, el) => {
      requester ! ContainsResult(id, el == elem && !removed)
    }
    case Remove(requester, id, el) => {
      if (el == elem) removed = true
      requester ! OperationFinished(id)
    }
    case CopyTo(newRoot) => {
      if (!removed) newRoot ! Insert(self, elem, elem) // Copy this if not removed
      subtrees.values.foreach(_ ! CopyTo(newRoot)) // Copy children

      context.become(copying(subtrees.values.toSet, insertConfirmed = removed))
    }
  }

  def insert(elem: Int, pos: Position): Unit = {
    val ref = context.actorOf(props(elem, false))
    subtrees = subtrees + ((pos, ref))
  }

  // optional
  /**
   * `expected` is the set of ActorRefs whose replies we are waiting for,
   * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
   */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    if (insertConfirmed && expected.isEmpty) {
      context.parent ! CopyFinished
      context.become(normal) // Switch back to normal for future
      normal // Return normal for now
    } else {
      case OperationFinished(id) => if (id == elem) context.become(copying(expected, insertConfirmed = true))
      case CopyFinished          => context.become(copying(expected - sender, insertConfirmed))
    }
  }

}
