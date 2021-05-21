package it.unibo.big

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.Calendar

import it.unibo.big.Carpenter._
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.roaringbitmap.RoaringBitmap

import scala.collection.mutable


object PampaHD {

  def main(args: Array[String]): Unit = {
    val tbl = args(0)
    val minSup: Int = args(1).toInt
    val expansionThreshold: Int = args(2).toInt
    val nexecutors: Int = args(3).toInt
    val ncores: Int = args(4).toInt
    val maxram: String = args(5)
    run(tbl, minSup, expansionThreshold, nexecutors, ncores, maxram)
  }

  def run(tbl: String, supMin: Int, expansionThr: Int = 1, nexecutors: Int, ncores: Int, maxram: String, outstats: mutable.Map[String, Long] = mutable.Map()): Unit = {
    val partitions = nexecutors * ncores * 3
    /* ------------------------------------------------------------------------
     * Spark Environment
     * ------------------------------------------------------------------------ */
    val spark: SparkSession = SparkSession.builder()
      .appName("PampaHD")
      .master("yarn")
      .config("spark.shuffle.reduceLocality.enabled", value = false)
      .config("spark.executor.instances", nexecutors)
      .config("spark.executor.cores", ncores)
      .config("spark.executor.memory", maxram)
      .enableHiveSupport
      .getOrCreate
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    LogManager.getRootLogger.setLevel(Level.ERROR)

    //noinspetion ScalaStyle
    import spark.implicits._
    val conf = s"PAMPAHD_${tbl}_${supMin}_$expansionThr"
    val outTable: String = s"$conf"
    val outTable2: String = s"logs/PAMPAHD_stats.csv"
    val minSup: Broadcast[Int] = sc.broadcast(supMin)
    val maxIterations: Broadcast[Int] = sc.broadcast(expansionThr)

    println(s"\n--- Writing to \n\t$outTable\n\t$outTable2")
    spark.sql("use trajectory")

    val table = if (tbl.equals("geolife")) s"tmp_tf_mapped_${tbl}_5" else tbl
    val _dataSet: RDD[(Long, Set[Any])] = // generate the transposed dataset, this is not considered in the timing
      spark.sql(s"select distinct tid, itemid from $table").rdd
        .map(i => (i.get(1), i.get(0))) // (item, tid)
        .groupByKey(partitions)
        .filter(t => t._2.size >= minSup.value) // consider only the items with enough support
        .flatMap(t => t._2.map(c => (c, t._1))) // (tid, item)
        .groupBy(_._1)
        .map(t => t._2.map(_._2.asInstanceOf[Any]).toSet) // (tid, itemset)
        .zipWithIndex // new tid (compact)
        .map(t => (t._2, t._1))
        .persist(StorageLevel.MEMORY_AND_DISK_SER) // .localCheckpoint()


    val countRedundantOk = sc.longAccumulator
    val countLeft = sc.longAccumulator
    val countIterations = sc.longAccumulator
    var itemsetLeft = true
    var curIteration: Int = 0
    var FCP: Broadcast[Set[Itemset]] = sc.broadcast(Set())

    val _R: Broadcast[Array[Int]] = sc.broadcast(_dataSet.map(_._1).collect().map(_.toInt).sorted)
    val startupTime: Long = System.currentTimeMillis()
    var TTs: RDD[(TT, RoaringBitmap)] =
      _dataSet
        .flatMap(t => _R.value.map(r => (t._1, t._2, r)))
        .groupBy(t => t._3)
        .map(t => (TT(t._2.map(r => (r._1.toInt, r._2)), t._1), RoaringBitmap.bitmapOf(_R.value.drop(t._1 + 1): _*)))
    // sc.parallelize(_R.value.map(rid => (TT(_dataSet, rid), RoaringBitmap.bitmapOf(_R.value.drop(rid + 1):_*))))
    while (itemsetLeft) {
      countLeft.reset()
      curIteration += 1
      val tmpRes: RDD[(mutable.Set[Itemset], mutable.Set[(TT, RoaringBitmap)])] =
        TTs
          .repartition(partitions)
          .map({ case (tt: TT, r: RoaringBitmap) =>
            val _FCP: mutable.Set[Itemset] = mutable.Set() ++ FCP.value
            val _toExtend: mutable.Set[(TT, RoaringBitmap)] = mutable.Set()
            val _stats: mutable.Map[String, Long] = mutable.Map()
            minePattern(tt, r, minSup.value, 1, _FCP, stats = _stats, maxIterations = Some(maxIterations.value), toExtend = _toExtend)
            countIterations.add(_stats("iterations"))
            countRedundantOk.add(_FCP.size)
            countLeft.add(_toExtend.size)
            (_FCP, _toExtend)
          })
          .persist(StorageLevel.MEMORY_AND_DISK_SER) // .localCheckpoint()

      // Compute the RDD and store the itemsets produced so far
      println(s"\n- ${new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(Calendar.getInstance().getTime)} $conf Iteration: " + curIteration)
      // Merge the closed frequent itemsets and keep only the ones with the highest support
      FCP = sc.broadcast(
        tmpRes
          .flatMap(x => x._1.map(i => (i, i)))
          .reduceByKey((i1, i2) => if (i1.sup >= i2.sup) i1 else i2)
          .map(i => mutable.ArrayBuffer(i._1))
          .treeReduce(_ ++ _).toSet)
      // Continue with the iterations and update the transposed tables
      TTs = tmpRes.values.flatMap(x => x)
      println(s"--- ${new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(Calendar.getInstance().getTime)} Done. Itemsets: ${FCP.value.size}; Iterations: ${countIterations.value}")
      sc.getPersistentRDDs.keys.toVector.sorted.dropRight(1).foreach(id => sc.getPersistentRDDs(id).unpersist(true))
      itemsetLeft = countLeft.value > 0
    }

    sc.parallelize(FCP.value.toSeq)
      .zipWithIndex // .zipWithUniqueId
      .flatMap({ case (itemset: Itemset, uid: Long) => itemset.items.map(i => (uid, i.toString, itemset.items.size, itemset.sup)) })
      .toDF("itemsetid", "item", "size", "support").write.mode(SaveMode.Overwrite).saveAsTable(outTable)
    val elapsedTime = System.currentTimeMillis() - startupTime

    val fileExists = Files.exists(Paths.get(outTable2))
    val bw = new BufferedWriter(new FileWriter(new File(outTable2), fileExists))
    if (!fileExists) {
      bw.write("dataset,minSup,iterations,itemsets,expansionthr,partitions,time(ms),nexecutor,ncore,maxram\n".toLowerCase())
    }
    bw.write(s"$tbl,${minSup.value},${countIterations.value},${FCP.value.size},${maxIterations.value},$partitions,$elapsedTime,$nexecutors,$ncores,$maxram\n")
    bw.close()

    sc.getPersistentRDDs.foreach(i => i._2.unpersist())
    spark.catalog.clearCache()
    spark.sqlContext.clearCache()
    sc.stop
  }
}
