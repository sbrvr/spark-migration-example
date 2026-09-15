package com.databricks.migrationgenie.scalamigrationexample.commonutilities

import scala.reflect.ClassTag

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/** Reusable, domain-agnostic data-quality helpers.
  *
  * All checks operate on `RDD[T]` (spec §2: stay on the RDD API for row-level work)
  * and RETURN metrics or filtered RDDs — they never log or mutate silently. Callers
  * (Silver) decide the policy: log, drop, or fail.
  *
  * Referential-integrity helpers take a BROADCAST parent-key set so the child RDD is
  * filtered map-side with no shuffle/join (spec §2 broadcast conventions).
  */
object DataQualityChecks {

  /** True iff the RDD has at least one row. */
  def isNonEmpty[T](rdd: RDD[T]): Boolean = !rdd.isEmpty()

  /** Count rows whose key is null or blank. `key` may return any type; null and
    * empty-after-trim string forms both count as violations. */
  def countNullKeys[T](rdd: RDD[T])(key: T => Any): Long =
    rdd.filter { t =>
      val k = key(t)
      k == null || k.toString.trim.isEmpty
    }.count()

  /** Count how many key VALUES occur more than once (i.e. number of duplicated groups). */
  def countDuplicateKeys[T](rdd: RDD[T])(key: T => Any): Long =
    rdd.map(t => (key(t), 1L))
      .reduceByKey(_ + _)
      .filter(_._2 > 1L)
      .count()

  /** Keep one row per key (first wins). Requires ClassTags for the shuffle. */
  def dedupeByKey[T: ClassTag, K: ClassTag](rdd: RDD[T])(key: T => K): RDD[T] =
    rdd.keyBy(key).reduceByKey((first, _) => first).values

  /** Count child rows whose foreign key is absent from the broadcast parent-key set. */
  def missingReferences[T, K](child: RDD[T], parentKeys: Broadcast[Set[K]])(fk: T => K): Long =
    child.filter(t => !parentKeys.value.contains(fk(t))).count()

  /** Drop orphan child rows — keep only those whose FK exists in the broadcast set. */
  def dropOrphans[T, K](child: RDD[T], parentKeys: Broadcast[Set[K]])(fk: T => K): RDD[T] =
    child.filter(t => parentKeys.value.contains(fk(t)))

  /** Count all elements in a one-shot collection (used to size collected samples for logging). */
  def countAll[T](xs: TraversableOnce[T]): Long = xs.toIterator.length.toLong
}
