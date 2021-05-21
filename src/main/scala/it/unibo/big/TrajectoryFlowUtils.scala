package it.unibo.big
import org.apache.spark.Partitioner
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class DumbPartitioner(val numPartitions: Int, val histogram: Map[Int, Long]) extends Partitioner {
  private val tot: Long = histogram.values.sum // total sum
  private val avg: Double = 1.0 * tot / numPartitions // average values in key
  private var curPartition: Int = 0 // initial partition
  private var curSum: Long = 0 // current sum
  private var keyCount = 0
  val cumPartition: mutable.Map[Int, Long] = mutable.Map[Int, Long]()
  val cumKeyPartition: mutable.Map[Int, Long] = mutable.Map[Int, Long]()
  val partitions: Map[Int, Int] = histogram.map({case (key: Int, load: Long) =>
    keyCount += 1
    curSum += load // add the elements for this key
    val p = curPartition // set the current partition
    if (curSum > avg) {
      cumPartition += p -> curSum
      cumKeyPartition += p -> keyCount
      // if the number of values is above the threshold, than change partition for the next element
      curPartition += 1
      // ... and rest the current sum for the new partition
      curSum = 0
      keyCount = 0
    }
    (key, p)
  })
  cumPartition += curPartition -> curSum
  cumKeyPartition += curPartition -> keyCount

  val defaultPartition: Int = numPartitions - 1

  override def getPartition(key: Any): Int = {
    partitions.getOrElse(key.asInstanceOf[Int], 0)
  }
}

class SortedPartitioner(val numPartitions: Int, val histogram: Map[Int, Long]) extends Partitioner {
  val cumPartition: mutable.Map[Int, Long] = mutable.Map() ++ (0 until numPartitions).map(i => i -> 0L).toMap
  val cumKeyPartition: mutable.Map[Int, Int] = mutable.Map() ++ (0 until numPartitions).map(i => i -> 0).toMap
  val partitions: Map[Int, Int] = histogram.toVector.sortBy(-_._2).map({case (key: Int, load: Long) =>
    val partition: (Int, Long) = cumPartition.minBy(i => (i._2, i._1))
    cumPartition    += partition._1 -> (partition._2 + load)
    cumKeyPartition += partition._1 -> (cumKeyPartition(partition._1) + 1)
    (key, partition._1)
  }).toMap

  val defaultPartition: Int = cumPartition.minBy(i => (i._2, -i._1))._1

  override def getPartition(key: Any): Int = {
    partitions.getOrElse(key.asInstanceOf[Int], defaultPartition)
  }
}

class LocalPartitioner(val numPartitions: Int, val keys: Set[Int]) extends Partitioner {
  private val kPerPartition: Int = Math.max(1, Math.ceil(1.0 * keys.size / numPartitions).toInt)
  private var p: Int = 0
  private var count: Int = 0
  private val partitioning: Map[Int, Int] = keys.toVector.sorted.map(i => {
    if (count == kPerPartition) {
      count = 0
      p += 1
    }
    count += 1
    i -> p
  }).toMap

  override def getPartition(key: Any): Int = {
    partitioning(key.asInstanceOf[Int])
  }
}

class CustomAccumulator(var accMap: Map[Int, Long] = Map[Int, Long]()) extends AccumulatorV2[Int, Map[Int, Long]] {
  private val myAcc: mutable.Map[Int, Long] = mutable.Map() ++ accMap
  def reset(): Unit = myAcc.clear()
  def add(key: Int): Unit = myAcc.put(key, myAcc.getOrElse(key, 0L) + 1L)
  def value: Map[Int, Long] = myAcc.toMap
  def isZero: Boolean = myAcc.isEmpty
  def copy(): CustomAccumulator = new CustomAccumulator(myAcc.toMap)
  def merge(other: AccumulatorV2[Int, Map[Int, Long]]): Unit = myAcc ++= other.value
}

class StatsAccumulator(val myAcc: (Int, Int, Int, Long, Long, Long) = (0, Int.MinValue, Int.MaxValue, 0, 0, 0)) extends AccumulatorV2[(Int, Int, Int, Long, Long, Long), (Int, Int, Int, Long, Long, Long)] {
  private val default = (0, Int.MinValue, Int.MaxValue, 0L, 0L, 0L)
  private var a: (Int, Int, Int, Long, Long, Long) = myAcc
  def reset(): Unit = a = default
  def add(b: (Int, Int, Int, Long, Long, Long)): Unit = a = (a._1 + b._1, Math.max(a._2, b._2), Math.min(a._3, b._3), a._4 + b._4, a._5 + b._5, a._6 + b._6)
  def value: (Int, Int, Int, Long, Long, Long) = a
  def isZero: Boolean = a == default
  def copy(): StatsAccumulator = new StatsAccumulator(a)
  def merge(other: AccumulatorV2[(Int, Int, Int, Long, Long, Long), (Int, Int, Int, Long, Long, Long)]): Unit = {
    add(other.value) // a = (a._1 + b._1, Math.max(a._2, b._2), Math.min(a._3, b._3), a._4 + b._4, a._5 + b._5, a._6 + b._6)
  }
}

/**
 * Trait to express the variable temporal scales that can be adopted by the algorithm.
 */
sealed trait TemporalScale {
  /** The identifier, as a string, of the time model. */
  val value: String
}

/** Temporal scale */
object TemporalScale {

  /**
   * Create scale from string.
   * @param value scale value
   * @return scale
   */
  def apply(value: String): TemporalScale = value match {
    case WeeklyScale.value => WeeklyScale
    case DailyScale.value => DailyScale
    case AbsoluteScale.value => AbsoluteScale
    case NoScale.value => NoScale
    case _ => throw new IllegalArgumentException("Unknown time scale: " + value)
  }

  case object WeeklyScale extends TemporalScale {
    override val value: String = "weekly"
  }

  case object DailyScale extends TemporalScale {
    override val value: String = "daily"
  }

  case object AbsoluteScale extends TemporalScale {
    override val value: String = "absolute"
  }

  case object NoScale extends TemporalScale {
    override val value: String = "notime"
  }
}

