/**
 *  Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.cluster

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import akka.actor.Address

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ReachabilitySpec extends WordSpec with MustMatchers {

  import Reachability.{ Reachable, Unreachable, Terminated, Record }

  val nodeA = UniqueAddress(Address("akka.tcp", "sys", "a", 2552), 1)
  val nodeB = UniqueAddress(Address("akka.tcp", "sys", "b", 2552), 2)
  val nodeC = UniqueAddress(Address("akka.tcp", "sys", "c", 2552), 3)
  val nodeD = UniqueAddress(Address("akka.tcp", "sys", "d", 2552), 4)
  val nodeE = UniqueAddress(Address("akka.tcp", "sys", "e", 2552), 5)

  "Reachability table" must {

    "be reachable when empty" in {
      val r = Reachability.empty
      r.isReachable(nodeA) must be(true)
      r.allUnreachable must be(Set.empty)
    }

    "be unreachable when one observed unreachable" in {
      val r = Reachability.empty.unreachable(nodeB, nodeA)
      r.isReachable(nodeA) must be(false)
      r.allUnreachable must be(Set(nodeA))
    }

    "be not be reachable when terminated" in {
      val r = Reachability.empty.terminated(nodeB, nodeA)
      r.isReachable(nodeA) must be(false)
      // allUnreachable doesn't include terminated
      r.allUnreachable must be(Set.empty)
    }

    "not change terminated entry" in {
      val r = Reachability.empty.terminated(nodeB, nodeA)
      r.reachable(nodeB, nodeA) must be theSameInstanceAs (r)
      r.unreachable(nodeB, nodeA) must be theSameInstanceAs (r)
    }

    "not change when same status" in {
      val r = Reachability.empty.unreachable(nodeB, nodeA)
      r.unreachable(nodeB, nodeA) must be theSameInstanceAs (r)
    }

    "be unreachable when some observed unreachable and others reachable" in {
      val r = Reachability.empty.unreachable(nodeB, nodeA).unreachable(nodeC, nodeA).reachable(nodeD, nodeA)
      r.isReachable(nodeA) must be(false)
    }

    "be reachable when all observed reachable again" in {
      val r = Reachability.empty.unreachable(nodeB, nodeA).unreachable(nodeC, nodeA).
        reachable(nodeB, nodeA).reachable(nodeC, nodeA).
        unreachable(nodeB, nodeC).unreachable(nodeC, nodeB)
      r.isReachable(nodeA) must be(true)
    }

    "have correct aggregated status" in {
      val records = Vector(
        Reachability.Record(nodeA, nodeB, Reachable, 2),
        Reachability.Record(nodeC, nodeB, Unreachable, 2),
        Reachability.Record(nodeA, nodeD, Unreachable, 2),
        Reachability.Record(nodeC, nodeB, Terminated, 2))
      val r = Reachability(records)
      r.status(nodeA) must be(Reachable)
      r.status(nodeB) must be(Terminated)
      r.status(nodeD) must be(Unreachable)
    }

    "have correct status for a mix of nodes" in {
      val r = Reachability.empty.
        unreachable(nodeB, nodeA).unreachable(nodeC, nodeA).unreachable(nodeD, nodeA).
        unreachable(nodeC, nodeB).reachable(nodeC, nodeB).unreachable(nodeD, nodeB).
        unreachable(nodeD, nodeC).reachable(nodeD, nodeC).
        reachable(nodeE, nodeD).
        unreachable(nodeA, nodeE).terminated(nodeB, nodeE)

      r.status(nodeB, nodeA) must be(Unreachable)
      r.status(nodeC, nodeA) must be(Unreachable)
      r.status(nodeD, nodeA) must be(Unreachable)

      r.status(nodeC, nodeB) must be(Reachable)
      r.status(nodeD, nodeB) must be(Unreachable)

      r.status(nodeA, nodeE) must be(Unreachable)
      r.status(nodeB, nodeE) must be(Terminated)

      r.isReachable(nodeA) must be(false)
      r.isReachable(nodeB) must be(false)
      r.isReachable(nodeC) must be(true)
      r.isReachable(nodeD) must be(true)
      r.isReachable(nodeE) must be(false)

      r.allUnreachable must be(Set(nodeA, nodeB))
      r.allUnreachableFrom(nodeA) must be(Set(nodeE))
      r.allUnreachableFrom(nodeB) must be(Set(nodeA))
      r.allUnreachableFrom(nodeC) must be(Set(nodeA))
      r.allUnreachableFrom(nodeD) must be(Set(nodeA, nodeB))

      r.observersGroupedByUnreachable must be(Map(
        nodeA -> Set(nodeB, nodeC, nodeD),
        nodeB -> Set(nodeD),
        nodeE -> Set(nodeA)))
    }

    "merge both empty" in {
      val r1 = Reachability.empty
      val r2 = Reachability.empty
      r1.merge(Set(nodeA, nodeB), r2) must be theSameInstanceAs (r1)
    }

    "merge one empty" in {
      val r1 = Reachability.empty.unreachable(nodeB, nodeA)
      val r2 = Reachability.empty
      r1.merge(Set(nodeA, nodeB, nodeC), r2) must be theSameInstanceAs (r1)
      r2.merge(Set(nodeA, nodeB, nodeC), r1) must be theSameInstanceAs (r1)
    }

    "merge by picking latest version of each record" in {
      val r1 = Reachability.empty.unreachable(nodeB, nodeA).unreachable(nodeC, nodeD)
      val r2 = r1.reachable(nodeB, nodeA).unreachable(nodeD, nodeE).unreachable(nodeC, nodeA)
      val merged = r1.merge(Set(nodeA, nodeB, nodeC, nodeD, nodeE), r2)

      merged.status(nodeB, nodeA) must be(Reachable)
      merged.status(nodeC, nodeA) must be(Unreachable)
      merged.status(nodeC, nodeD) must be(Unreachable)
      merged.status(nodeD, nodeE) must be(Unreachable)
      merged.status(nodeE, nodeA) must be(Reachable)

      merged.isReachable(nodeA) must be(false)
      merged.isReachable(nodeD) must be(false)
      merged.isReachable(nodeE) must be(false)
    }

    "remove node" in {
      val r = Reachability.empty.
        unreachable(nodeB, nodeA).
        unreachable(nodeC, nodeD).
        unreachable(nodeB, nodeC).
        unreachable(nodeB, nodeE).
        remove(Set(nodeA, nodeB))

      r.status(nodeB, nodeA) must be(Reachable)
      r.status(nodeC, nodeD) must be(Unreachable)
      r.status(nodeB, nodeC) must be(Reachable)
      r.status(nodeB, nodeE) must be(Reachable)
    }

  }
}
