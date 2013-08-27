/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.cluster

import scala.annotation.tailrec
import scala.collection.immutable
import scala.collection.breakOut
import akka.actor.Address

/**
 * INTERNAL API
 */
private[cluster] object Reachability {
  private val emptyObserverRows: Map[UniqueAddress, Record] =
    Map.empty.withDefault(subject ⇒ Record(null, subject, Reachable, 0))
  private val emptyTable: Map[UniqueAddress, Map[UniqueAddress, Record]] =
    Map.empty.withDefaultValue(emptyObserverRows)
  val empty = new Reachability(Vector.empty, None, None)

  def apply(records: immutable.IndexedSeq[Record]): Reachability = new Reachability(records, None, None)

  def apply(records: immutable.Seq[Record]): Reachability = records match {
    case r: immutable.IndexedSeq[Record] ⇒ apply(r)
    case _                               ⇒ apply(records.toVector)
  }

  case class Record(observer: UniqueAddress, subject: UniqueAddress, status: ReachabilityStatus, version: Long)

  sealed trait ReachabilityStatus
  case object Reachable extends ReachabilityStatus
  case object Unreachable extends ReachabilityStatus
  case object Terminated extends ReachabilityStatus

}

/**
 * INTERNAL API
 *
 * Immutable data structure that holds the reachability status of subject nodes as seen
 * from observer nodes. Failure detector for the subject nodes exist on the
 * observer nodes. Changes (reachable, unreachable, terminated) are only performed
 * by observer nodes to its own records. Each change bumps the version number of the
 * record, and thereby it is always possible to determine which record is newest when
 * merging two instances.
 *
 * Aggregated status of a subject node is defined as (in this order):
 * - Terminated if any observer node considers it as Terminated
 * - Unreachable if any observer node considers it as Unreachable
 * - Reachable otherwise, i.e. no observer node considers it as Unreachable
 */
private[cluster] class Reachability private (
  private val records: immutable.IndexedSeq[Reachability.Record],
  precomputedTable: Option[Map[UniqueAddress, Map[UniqueAddress, Reachability.Record]]],
  precomputedAggregatedStatus: Option[Map[UniqueAddress, Reachability.ReachabilityStatus]]) extends Serializable {

  import Reachability._

  // lookup cache with observer as key, and records by subject as value
  @transient private lazy val table: Map[UniqueAddress, Map[UniqueAddress, Reachability.Record]] =
    precomputedTable match {
      case Some(x) ⇒ x
      case None ⇒
        (records.groupBy(_.observer).map {
          case (observer, observerRows) ⇒
            val observerMap: Map[UniqueAddress, Record] =
              (observerRows.map { row ⇒ (row.subject -> row) })(breakOut)
            (observer -> observerMap.withDefault(subject ⇒ Record(observer, subject, Reachable, 0)))
        }).withDefaultValue(emptyObserverRows)
    }

  @transient private lazy val aggregatedStatus: Map[UniqueAddress, Reachability.ReachabilityStatus] = precomputedAggregatedStatus match {
    case Some(x) ⇒ x
    case None ⇒
      records.map(_.subject).map(subject ⇒ subject -> aggregateStatus(subject, records.iterator))(breakOut)
  }

  @tailrec private def aggregateStatus(subject: UniqueAddress, records: Iterator[Record]): ReachabilityStatus = {
    if (records.hasNext) {
      val r = records.next()
      if ((r.status == Unreachable || r.status == Terminated) && r.subject == subject) r.status
      else aggregateStatus(subject, records)
    } else Reachable
  }

  def unreachable(observer: UniqueAddress, subject: UniqueAddress): Reachability =
    change(observer, subject, Unreachable)

  def reachable(observer: UniqueAddress, subject: UniqueAddress): Reachability =
    change(observer, subject, Reachable)

  def terminated(observer: UniqueAddress, subject: UniqueAddress): Reachability =
    change(observer, subject, Terminated)

  private def change(observer: UniqueAddress, subject: UniqueAddress, status: ReachabilityStatus): Reachability = {
    val oldObserverRows = table(observer)
    val oldRecord = oldObserverRows(subject)
    if (oldRecord.status == Terminated || oldRecord.status == status)
      this
    else {
      val newRecord = Record(observer, subject, status, oldRecord.version + 1)
      val newRecords = records.indexOf(oldRecord) match {
        case -1 ⇒ records :+ newRecord
        case i  ⇒ records.updated(i, newRecord)
      }
      val newObserverRows = oldObserverRows.updated(subject, newRecord)
      val newTable = table.updated(observer, newObserverRows)

      val newStatus = aggregateStatus(subject, newTable.values.flatMap(_.values).iterator)
      val newAggregatedStatus = aggregatedStatus.updated(subject, newStatus)

      new Reachability(newRecords, Some(newTable), Some(newAggregatedStatus))
    }
  }

  def merge(allowed: immutable.Set[UniqueAddress], other: Reachability): Reachability = {
    if (records.isEmpty) other
    else if (other.records.isEmpty) this
    else {
      val recordBuilder = new immutable.VectorBuilder[Record]
      allowed foreach { observer ⇒
        (table.get(observer), other.table.get(observer)) match {
          case (None, None)               ⇒
          case (Some(rows1), Some(rows2)) ⇒ mergeObserverRows(rows1, rows2, recordBuilder)
          case (Some(rows1), None)        ⇒ recordBuilder ++= rows1.valuesIterator
          case (None, Some(rows2))        ⇒ recordBuilder ++= rows2.valuesIterator
        }
      }

      new Reachability(recordBuilder.result(), None, None)
    }
  }

  private def mergeObserverRows(
    rows1: Map[UniqueAddress, Reachability.Record], rows2: Map[UniqueAddress, Reachability.Record],
    recordBuilder: immutable.VectorBuilder[Record]): Unit = {

    val allSubjects = rows1.keySet ++ rows2.keySet
    allSubjects foreach { subject ⇒
      (rows1.get(subject), rows2.get(subject)) match {
        case (Some(r1), Some(r2)) ⇒
          recordBuilder += (if (r1.version > r2.version) r1 else r2)
        case (Some(r1), None) ⇒
          recordBuilder += r1
        case (None, Some(r2)) ⇒
          recordBuilder += r2
        case (None, None) ⇒
          throw new IllegalStateException(s"Unexpected [$subject]")
      }
    }
  }

  def remove(nodes: Iterable[UniqueAddress]): Reachability = {
    val nodesSet = nodes.to[immutable.HashSet]
    val oldRecords = allRecords
    val newRecords = oldRecords.filterNot(r ⇒ nodesSet(r.observer) || nodesSet(r.subject))
    if (newRecords.size == oldRecords.size) this
    else Reachability(newRecords)
  }

  def status(observer: UniqueAddress, subject: UniqueAddress): ReachabilityStatus =
    table(observer)(subject).status

  def isReachable(node: UniqueAddress): Boolean =
    aggregatedStatus.get(node) match {
      case None            ⇒ true
      case Some(Reachable) ⇒ true
      case _               ⇒ false
    }

  def isReachable(observer: UniqueAddress, subject: UniqueAddress): Boolean =
    status(observer, subject) == Reachable

  def isAllReachable: Boolean = aggregatedStatus.isEmpty

  /**
   * Doesn't include terminated
   */
  def allUnreachable: Set[UniqueAddress] = collect(Unreachable)

  private def collect(status: ReachabilityStatus): Set[UniqueAddress] = {
    val result: immutable.HashSet[UniqueAddress] = aggregatedStatus.collect {
      case (subject, _status) if _status == status ⇒ subject
    }(breakOut)
    result
  }

  /**
   * Doesn't include terminated
   */
  def allUnreachableFrom(observer: UniqueAddress): Set[UniqueAddress] = {
    val result: immutable.HashSet[UniqueAddress] = table(observer).collect {
      case (subject, record) if record.status == Unreachable ⇒ subject
    }(breakOut)
    result
  }

  def observersGroupedByUnreachable: Map[UniqueAddress, Set[UniqueAddress]] = {
    allRecords.groupBy(_.subject).collect {
      case (subject, records) if records.exists(_.status == Unreachable) ⇒
        val observers: Set[UniqueAddress] =
          records.collect { case r if r.status == Unreachable ⇒ r.observer }(breakOut)
        (subject -> observers)
    }
  }

  def allObservers: Set[UniqueAddress] = table.keys.toSet

  def allSubjects: Set[UniqueAddress] = aggregatedStatus.keys.toSet

  def allRecords: immutable.IndexedSeq[Record] = table.values.flatMap(_.values)(breakOut)

  override def hashCode: Int = table.hashCode

  override def equals(obj: Any): Boolean = obj match {
    case other: Reachability ⇒ records.size == other.records.size && table == other.table
    case _                   ⇒ false
  }

  override def toString: String = {
    val rows = for {
      observer ← table.keys.toSeq.sorted
      observerRows = table(observer)
      subject ← observerRows.keys.toSeq.sorted
    } yield {
      val record = observerRows(subject)
      val aggregated = aggregatedStatus.get(subject).getOrElse("")
      s"${observer.address} -> ${subject.address}: ${record.status} [$aggregated] (${record.version})"
    }

    rows.mkString(", ")
  }

}

