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
  val empty = new Reachability(Vector.empty, None, None)

  def apply(records: immutable.IndexedSeq[Record]): Reachability = new Reachability(records, None, None)

  def apply(records: immutable.Seq[Record]): Reachability = records match {
    case r: immutable.IndexedSeq[Record] ⇒ apply(r)
    case _                               ⇒ apply(records.toVector)
  }

  @SerialVersionUID(1L)
  case class Record(observer: UniqueAddress, subject: UniqueAddress, status: ReachabilityStatus, version: Long)

  sealed trait ReachabilityStatus
  @SerialVersionUID(1L) case object Reachable extends ReachabilityStatus
  @SerialVersionUID(1L) case object Unreachable extends ReachabilityStatus
  @SerialVersionUID(1L) case object Terminated extends ReachabilityStatus

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
@SerialVersionUID(1L)
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
        records.groupBy(_.observer).map {
          case (observer, observerRows) ⇒
            val observerMap: Map[UniqueAddress, Record] =
              (observerRows.map { row ⇒ (row.subject -> row) })(breakOut)
            (observer -> observerMap)
        }
    }

  @transient private lazy val aggregatedStatus: Map[UniqueAddress, Reachability.ReachabilityStatus] = precomputedAggregatedStatus match {
    case Some(x) ⇒ x
    case None ⇒
      records.map(_.subject).map(subject ⇒ subject -> aggregateStatus(subject, records.iterator))(breakOut)
  }

  @tailrec private def aggregateStatus(subject: UniqueAddress, records: Iterator[Record],
                                       foundUnreachable: Boolean = false): ReachabilityStatus = {
    if (records.hasNext) {
      val r = records.next()
      if (r.subject != subject) aggregateStatus(subject, records, foundUnreachable)
      else if (r.status == Terminated) r.status
      else if (r.status == Unreachable) aggregateStatus(subject, records, true)
      else aggregateStatus(subject, records, foundUnreachable)
    } else {
      if (foundUnreachable) Unreachable else Reachable
    }
  }

  def unreachable(observer: UniqueAddress, subject: UniqueAddress): Reachability =
    change(observer, subject, Unreachable)

  def reachable(observer: UniqueAddress, subject: UniqueAddress): Reachability =
    change(observer, subject, Reachable)

  def terminated(observer: UniqueAddress, subject: UniqueAddress): Reachability =
    change(observer, subject, Terminated)

  private def change(observer: UniqueAddress, subject: UniqueAddress, status: ReachabilityStatus): Reachability = {
    table.get(observer) match {
      case None ⇒
        val newRecord = Record(observer, subject, status, 1)
        val newRecords = records :+ newRecord
        val newTable = table.updated(observer, Map(subject -> newRecord))
        val newAggregatedStatus = aggregatedStatus.updated(subject,
          aggregateStatus(subject, newRecords.iterator))
        new Reachability(newRecords, Some(newTable), Some(newAggregatedStatus))

      case Some(oldObserverRows) ⇒

        def updatedTable(newRecord: Record) = {
          val newObserverRows = oldObserverRows.updated(newRecord.subject, newRecord)
          table.updated(newRecord.observer, newObserverRows)
        }

        oldObserverRows.get(subject) match {
          case None ⇒
            val newRecord = Record(observer, subject, status, 1)
            val newRecords = records :+ newRecord
            val newTable = updatedTable(newRecord)
            val newAggregatedStatus = aggregatedStatus.updated(subject,
              aggregateStatus(subject, newRecords.iterator))
            new Reachability(newRecords, Some(newTable), Some(newAggregatedStatus))
          case Some(oldRecord) ⇒
            if (oldRecord.status == Terminated || oldRecord.status == status)
              this
            else {
              val newRecord = Record(observer, subject, status, oldRecord.version + 1)
              val newRecords = records.updated(records.indexOf(oldRecord), newRecord)
              val newTable = updatedTable(newRecord)
              val newAggregatedStatus = aggregatedStatus.updated(subject,
                aggregateStatus(subject, newRecords.iterator))
              new Reachability(newRecords, Some(newTable), Some(newAggregatedStatus))
            }
        }
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
    table.get(observer) match {
      case None ⇒ Reachable
      case Some(observerRows) ⇒ observerRows.get(subject) match {
        case None         ⇒ Reachable
        case Some(record) ⇒ record.status
      }
    }

  def status(node: UniqueAddress): ReachabilityStatus =
    aggregatedStatus.getOrElse(node, Reachable)

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
  def allUnreachableFrom(observer: UniqueAddress): Set[UniqueAddress] =
    table.get(observer) match {
      case None ⇒ Set.empty
      case Some(observerRows) ⇒
        val result: immutable.HashSet[UniqueAddress] = observerRows.collect {
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

  def allObservers: Set[UniqueAddress] = table.keySet

  def allSubjects: Set[UniqueAddress] = aggregatedStatus.keySet

  def allRecords: immutable.IndexedSeq[Record] = records

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

