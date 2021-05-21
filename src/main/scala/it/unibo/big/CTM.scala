package it.unibo.big

import it.unibo.big.Main._
import it.unibo.big.Utils._
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.roaringbitmap.RoaringBitmap

import java.text.SimpleDateFormat
import java.util.{Calendar, UUID}

class CTM {}

object CTM {
    private val L = Logger.getLogger(classOf[CTM])

    def CTM(spark: SparkSession, trans: RDD[(Tid, Itemid)], brdNeighborhood: Option[Broadcast[Map[Tid, RoaringBitmap]]], mSup: Int, mCrd: Int, isPlatoon: Boolean): (Long, Array[(RoaringBitmap, Tid, Tid)], Long) = {
        if (trans.count() == 0) {
            return (0, Array(), 0)
        }
        val acc = spark.sparkContext.longAccumulator
        val acc2 = spark.sparkContext.longAccumulator
        val accmCrd = spark.sparkContext.longAccumulator
        val accmLen = spark.sparkContext.longAccumulator
        val accmSup = spark.sparkContext.longAccumulator

        /* *****************************************************************************************************************
         * BROADCASTING TRAJECTORIES IN CELL. This is the TT' used as transposed table as defined by Carpenter algorithm.
         * Also use RoaringbitMap for performance reason
         * ****************************************************************************************************************/
        // print("--- Broadcasting brdTrajInCell (i.e., itemInTid)... ")
        val brdTrajInCell: Broadcast[Map[Tid, RoaringBitmap]] = spark.sparkContext.broadcast(
            trans
                .mapValues({ itemid: Itemid => RoaringBitmap.bitmapOf(itemid) })
                .reduceByKey((anItem, anotherItem) => {
                    val m = RoaringBitmap.or(anItem, anotherItem)
                    // m.runOptimize()
                    m
                }) // group by all the trajectories in the same cell
                .map(i => Array(i._1 -> i._2))
                .treeReduce(_ ++ _)
                .toMap
        )
        val nTransactions: Long = brdTrajInCell.value.size
        /* *****************************************************************************************************************
         * END - BROADCASTING TRAJECTORIES IN CELL
         * ****************************************************************************************************************/

        /**
         * Given a set of trajectoryIDs, create a bitmap containing all the common cells between all the Trajectories.
         *
         * @param itemset a set of trajectoryIDs.
         * @return a Bitmap containing all the cells common to all the trajectories specified by the input.
         */
        def support(itemset: RoaringBitmap, prevSupport: Option[RoaringBitmap] = None): RoaringBitmap = {
            val res = RoaringBitmap.bitmapOf()
            brdTrajInCell.value.foreach({ case (tid, transaction) =>
                if (prevSupport.isEmpty || !prevSupport.get.contains(tid)) {
                    val iterator = itemset.getIntIterator
                    var isOk = true
                    while (isOk && iterator.hasNext) {
                        isOk = transaction.contains(iterator.next())
                    }
                    if (isOk) {
                        res.add(tid)
                    }
                } else {
                    res.add(tid)
                }
            })
            res
        }

        /**
         * For test purpose only. Do not invoke this function otherwise
         * @param lCluster
         * @param lClusterSupport
         * @param XplusY
         * @param R
         * @param key
         */
        def checkFilters(lCluster: RoaringBitmap, lClusterSupport: RoaringBitmap, XplusYplusKey: RoaringBitmap, R: RoaringBitmap, key: Integer): Unit = {
            if (!isValid(RoaringBitmap.or(XplusYplusKey, R), mSup, brdNeighborhood)) {
                accmLen.add(1)
            }
            val c = RoaringBitmap.and(lCluster, brdTrajInCell.value(key)) // compute the new co-movement pattern
            val newClusterSupport = support(c, Some(lClusterSupport)) // compute its support
            if (RoaringBitmap.and(lCluster, brdTrajInCell.value(key)).getCardinality < mCrd) {
                accmCrd.add(1)
            }
            if (!isNonRedundant(XplusYplusKey, R, newClusterSupport)) { // if it is *potentially* a valid co-movement pattern...
                accmSup.add(1)
            }
        }

        /* *****************************************************************************************************************
         * Setting up the first sItemset definition.
         * ****************************************************************************************************************/
        var curIteration: Int = 0
        val countOk = spark.sparkContext.longAccumulator
        val countToExtend = spark.sparkContext.longAccumulator
        var countStored: Long = 0
        var nItemsets: Long = 0
        var c: Long = 0

        CustomTimer.start()
        var clusters: RDD[CarpenterRowSet] =
            trans
                .mapValues({ trajId: Itemid => RoaringBitmap.bitmapOf(trajId) }) // This map the CellID -> TrajID converting the TrajID into a RoraingBitMap
                .reduceByKey((aTrajectory, bTrajectory) => RoaringBitmap.or(aTrajectory, bTrajectory)) // group by all the trajectories in the same cell
                .flatMap({ case (key: Tid, lCluster: RoaringBitmap) => // Now this is a Real TT' as intended by John Carpenter itself
                    val X: RoaringBitmap = RoaringBitmap.bitmapOf(key) // current transaction
                    val R: RoaringBitmap = RoaringBitmap.bitmapOf(brdTrajInCell.value.filter({ case (otherKey: Tid, _: RoaringBitmap) => key < otherKey }).toArray.map(_._1.toInt): _*) // select only the new cells
                    Array((lCluster, true, X, R, support(lCluster)))
                })
                .filter({ case (i: RoaringBitmap, extend: Boolean, cs: RoaringBitmap, sp: RoaringBitmap, ts: RoaringBitmap) => isExtendable(i, cs, sp, ts, mCrd, mSup, brdNeighborhood) })
                .localCheckpoint()
        val clusterCount = clusters.count()
        println(s"\n--- ${new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(Calendar.getInstance().getTime)} Init clusters: $clusterCount")
        countOk.reset()
        countToExtend.reset()
        /* *****************************************************************************************************************
         * END - Creating the transactional dataset
         * ****************************************************************************************************************/

        val empty: RoaringBitmap = RoaringBitmap.bitmapOf()
        do {
            curIteration += 1
            L.info(s"\n--- ${new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(Calendar.getInstance().getTime)} $conf Iteration: " + curIteration)
            countOk.reset()
            countToExtend.reset()
            clusters =
                (if (curIteration % repfreq == 0) {
                    clusters.repartition(partitions)
                } else {
                    clusters
                }) // Shuffle the data every few iterations
                    .flatMap({
                        case (lCluster: RoaringBitmap, extend: Boolean, x: RoaringBitmap, r: RoaringBitmap, lClusterSupport: RoaringBitmap) =>
                            if (extend) { // If the sItemset should be extended...
                                acc.add(1)
                                var L: Array[CarpenterRowSet] = Array() // accumulator
                                // cannot check this after connectedComponent(RoaringBitmap.or(XplusY, R)), otherwise platoon fails
                                if (isValid(lClusterSupport, mSup, brdNeighborhood)) { // if is a valid co-movement pattern...
                                    countOk.add(1) // update the counter
                                    countToExtend.add(-1) // decrease the counter to extend to avoid double counting (since later L.size will contain this pattern)
                                    L +:= (lCluster, false, empty, empty, empty)
                                }
                                val Y: RoaringBitmap = RoaringBitmap.and(lClusterSupport, r) // tiles shared by all transactions
                                val XplusY: RoaringBitmap = RoaringBitmap.or(x, Y) // ... are directly added to the current transaction
                                val R: RoaringBitmap = RoaringBitmap.andNot(r, Y) // ... and remove from the remaining transactions
                                var Rnew: RoaringBitmap = R
                                R.forEach(toJavaConsumer({ key: Integer => {
                                    acc2.add(1)
                                    // BEGIN - TESTING: For test purpose only, comment this function otherwise
                                    // checkFilters(lCluster, lClusterSupport, XplusYplusKey, Rnew, key)
                                    // END - TESTING
                                    // if (isValid(RoaringBitmap.or(XplusYplusKey, Rnew), mSup, brdNeighborhood)) { // if CT \cup RT contains a potentially valid pattern
                                    if (XplusY.getCardinality + Rnew.getCardinality >= mSup) {
                                        val c = RoaringBitmap.and(lCluster, brdTrajInCell.value(key)) // compute the new co-movement pattern
                                        if (c.getCardinality >= mCrd && // if the pattern has a valid cardinality
                                            isValid(RoaringBitmap.or(XplusY, Rnew), mSup, brdNeighborhood)) { // if CT \cup RT contains a potentially valid pattern
                                            Rnew = RoaringBitmap.remove(Rnew, key, key + 1) // reduce the remaining transactions
                                            val XplusYplusKey: RoaringBitmap = RoaringBitmap.add(XplusY, key, key + 1) // update the covered transactions
                                            val newClusterSupport: RoaringBitmap = support(c, Some(lClusterSupport)) // compute its support
                                            if (isNonRedundant(XplusYplusKey, Rnew, newClusterSupport)) { // if it is *potentially* a valid co-movement pattern...
                                                L +:= (c, true, XplusYplusKey, Rnew, newClusterSupport) // store it
                                            }
                                        }
                                    }
                                }
                                }))
                                countToExtend.add(L.length)
                                L
                            } else {
                                if (!lCluster.hasRunCompression) lCluster.runOptimize()
                                Array((lCluster, extend, x, r, lClusterSupport))
                            }
                    })
                    .persist(StorageLevel.MEMORY_AND_DISK_SER) // .localCheckpoint()

            // Update clusters count values
            c = clusters.count()
            nItemsets += countOk.value
            println(s"--- ${new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(Calendar.getInstance().getTime)} Done: ${c + countStored} (ok: $nItemsets [stored: $countStored], toExtend: ${countToExtend.value})")

            /* ***************************************************************************************************************
             * Iteratively store the itemsets to free memory (if necessary)
             * **************************************************************************************************************/
            if (!returnResult && (countToExtend.value == 0 || nItemsets - countStored >= storage_thr)) {
                if (storage_thr > 0) {
                    L.debug(s"--- ${new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(Calendar.getInstance().getTime)} Writing itemsets to the database")
                    import spark.implicits._
                    val towrite: RDD[(String, RoaringBitmap)] = clusters
                        .filter({ case (_: RoaringBitmap, extend: Boolean, _: RoaringBitmap, _: RoaringBitmap, _: RoaringBitmap) => !extend })
                        .map(sItemset => (UUID.randomUUID().toString, sItemset._1))
                        .cache()

                    towrite
                        .map({ case (uid: String, itemset: RoaringBitmap) =>
                            (uid, itemset.getCardinality, support(itemset, Option.empty).getCardinality)
                        })
                        .toDF("itemsetid", "size", "support")
                        .write.mode(if (countStored == 0) SaveMode.Overwrite else SaveMode.Append).saveAsTable(summaryTable)

                    towrite
                        .flatMap({ case (uid: String, itemset: RoaringBitmap) =>
                            itemset.toArray.map(i => {
                                require(i >= 0, "itemid is below zero")
                                (uid, i)
                            })
                        })
                        .toDF("itemsetid", "itemid")
                        .write.mode(if (countStored == 0) SaveMode.Overwrite else SaveMode.Append).saveAsTable(itemsetTable)

                    towrite
                        .flatMap({ case (uid: String, itemset: RoaringBitmap) =>
                            support(itemset, Option.empty).toArray.map(i => {
                                require(i >= 0, "tileid is below zero")
                                (uid, i)
                            })
                        })
                        .toDF("itemsetid", "tileid")
                        .write.mode(if (countStored == 0) SaveMode.Overwrite else SaveMode.Append).saveAsTable(supportTable)
                    L.debug(s"--- ${new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(Calendar.getInstance().getTime)} Done writing")
                }
                countStored += nItemsets - countStored
                clusters = clusters.filter({ case (_: RoaringBitmap, extend: Boolean, _: RoaringBitmap, _: RoaringBitmap, _: RoaringBitmap) => extend })
            }
            // Remove useless checkpoints from memory
            spark.sparkContext.getPersistentRDDs.keys.toVector.sorted.dropRight(1).foreach(id => spark.sparkContext.getPersistentRDDs(id).unpersist(true))
        } while (countToExtend.value > 0)

        val res: Array[(RoaringBitmap, Int, Int)] =
            if (!returnResult) {
                Array()
            } else {
                clusters
                    // .filter({ case (_: RoaringBitmap, extend: Boolean, _: RoaringBitmap, _: RoaringBitmap) => !extend }) // This filter is mandatory to obtain only the interesting cells
                    .map({ case (i: RoaringBitmap, _: Boolean, _: RoaringBitmap, _: RoaringBitmap, _: RoaringBitmap) => Array[(RoaringBitmap, Int, Int)]((i, i.getCardinality, support(i).getCardinality)) })
                    .fold(Array.empty[(RoaringBitmap, Int, Int)])(_ ++ _)
            }
        writeStatsToFile(outTable2, inTable, mCrd, mSup, nItemsets, storage_thr, repfreq, limit, nexecutors, ncores, maxram, timeScale, bin_t, eps_t, bin_s, eps_s, nTransactions, brdTrajInCell.value.values.map(_.getSizeInBytes + 4).sum, if (brdNeighborhood.isEmpty) 0 else brdNeighborhood.get.value.values.map(_.getSizeInBytes + 4).sum, acc, acc2, accmLen, accmCrd, accmSup)
        spark.sparkContext.getPersistentRDDs.foreach(i => i._2.unpersist())
        spark.catalog.clearCache()
        spark.sqlContext.clearCache()
        (nItemsets, res, acc2.value)
    }
}
