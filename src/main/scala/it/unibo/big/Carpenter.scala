package it.unibo.big

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Paths}
import java.util.concurrent.{Callable, ExecutorService, Executors}
import com.google.common.base.Objects
import it.unibo.big.Carpenter.Itemset
import org.apache.spark.rdd.RDD
import org.roaringbitmap.RoaringBitmap

import scala.collection.mutable
import scala.io.Source

object Carpenter {

    def minePattern(ttX: TT, r: RoaringBitmap, minSup: Int, minCard: Int, FCP: mutable.Set[Itemset], stats: mutable.Map[String, Long], maxIterations: Option[Int] = None, toExtend: mutable.Set[(TT, RoaringBitmap)] = mutable.Set()): Unit = {
        stats += "iterations" -> (stats.getOrElse("iterations", 0L) + 1L)

        if (ttX.support + r.getCardinality < minSup) {
            return
        }

        var R: RoaringBitmap = r
        val Y: RoaringBitmap = ttX.Y
        R = RoaringBitmap.andNot(R, Y)

        val FX: Itemset = ttX.FX

        if (FX.items.size < minCard || FCP.contains(FX)) {
            return
        }

        if (ttX.support + Y.getCardinality >= minSup) {
            FCP += FX
        }

        var Rsorted: Array[Int] = R.toArray.sorted
        while (Rsorted.nonEmpty) {
            val ri = Rsorted.head
            Rsorted = Rsorted.drop(1)
            if (maxIterations.isEmpty || maxIterations.isDefined && stats("iterations") < maxIterations.get) {
                minePattern(TT(ttX, ri, Y.getCardinality), RoaringBitmap.bitmapOf(Rsorted: _*), minSup, minCard, FCP, stats = stats)
            } else {
                toExtend ++= Set((TT(ttX, ri, Y.getCardinality), RoaringBitmap.bitmapOf(Rsorted: _*)))
            }
        }
    }

    def run(d: Iterable[Set[Any]], minSup: Int, stats: mutable.Map[String, Long]): Set[Itemset] = {
        run(d, minSup, 1, stats)
    }

    def run(d: Iterable[Set[Any]], minSup: Int, minCard: Int, stats: mutable.Map[String, Long] = mutable.Map()): Set[Itemset] = {
        val _itemAboveThr: Set[Any] = d.toVector.flatten.groupBy(x => x).filter({ case (item: Any, freq: Iterable[Any]) => freq.size >= minSup }).keySet
        val _dataSet: Vector[(Int, Set[Any])] = d.map(r => r.intersect(_itemAboveThr)).filter(_.nonEmpty).zipWithIndex.map(t => (t._2, t._1)).toVector
        val _R: Vector[Int] = _dataSet.map(_._1).sorted
        val _FCP: mutable.Set[Itemset] = mutable.Set()
        val startTime = System.currentTimeMillis()
        _R.foreach(rid => minePattern(TT(_dataSet, rid), RoaringBitmap.bitmapOf(_R.drop(rid + 1): _*), minSup, minCard, _FCP, stats = stats))
        stats += "time(ms)" -> (System.currentTimeMillis() - startTime)
        _FCP.toSet
    }

    def run(filename: String, minSup: Int, minCard: Int): Set[Itemset] = {
        val data = Source.fromFile(filename).getLines.map(l => l.split(" ").map(_.trim.asInstanceOf[Any]).toSet).toList
        val stats: mutable.Map[String, Long] = mutable.Map()
        println(filename)
        val FCP = run(data, minSup, minCard, stats)
        val output = s"results/CARPENTER_stats.csv"
        val fileExists = Files.exists(Paths.get(output))
        val bw = new BufferedWriter(new FileWriter(new File(output), fileExists))
        if (!fileExists) {
            bw.write("dataset,minSup,minCard,iterations,itemsets,time(ms)\n".toLowerCase())
        }
        bw.write(s"${filename.replace("resources/", "").replace(".txt", "")},$minSup,$minCard,${stats("iterations")},${FCP.size},${stats("time(ms)")}\n")
        bw.close()
        FCP
    }

    def main(args: Array[String]): Unit = {
        //    val ex: ExecutorService = Executors.newFixedThreadPool(args(0).toInt)
        run("resources/closed0_trans.txt", 1, 1)
        run("resources/milan_dataset.csv", 12, 100)
        run("resources/milan_dataset.csv", 10, 100)
        run("resources/milan_dataset.csv", 8, 100)
        run("resources/milan_dataset.csv", 6, 100)
        run("resources/milan_dataset.csv", 12, 90)
        run("resources/milan_dataset.csv", 12, 80)
        run("resources/milan_dataset.csv", 12, 70)
        run("resources/milan_dataset.csv", 12, 60)
        //    (0 until 2).foreach(_ => {
        //      ex.submit(new Callable[Set[Itemset]] {
        //        def call(): Set[Itemset] = {
        //          run("resources/PAMPA_breastcancer_positive.txt", 15)
        //        }
        //      })
        //      ex.submit(new Callable[Set[Itemset]] {
        //        def call(): Set[Itemset] = {
        //          run("resources/PAMPA_breastcancer_positive.txt", 14)
        //        }
        //      })
        //      ex.submit(new Callable[Set[Itemset]] {
        //        def call(): Set[Itemset] = {
        //          run("resources/PAMPA_breastcancer_positive.txt", 13)
        //        }
        //      })
        //      ex.submit(new Callable[Set[Itemset]] {
        //        def call(): Set[Itemset] = {
        //          run("resources/PAMPA_breastcancer_positive.txt", 12)
        //        }
        //      })
        //      ex.submit(new Callable[Set[Itemset]] {
        //        def call(): Set[Itemset] = {
        //          run("resources/PAMPA_breastcancer_positive.txt", 11)
        //        }
        //      })
        //      ex.submit(new Callable[Set[Itemset]] {
        //        def call(): Set[Itemset] = {
        //          run("resources/PAMPA_breastcancer_positive.txt", 10)
        //        }
        //      })
        //      ex.submit(new Callable[Set[Itemset]] {
        //        def call(): Set[Itemset] = {
        //          run("resources/PAMPA_breastcancer_positive.txt", 9)
        //        }
        //      })
        //      ex.submit(new Callable[Set[Itemset]] {
        //        def call(): Set[Itemset] = {
        //          run("resources/PAMPA_breastcancer_positive.txt", 8)
        //        }
        //      })
        //      ex.submit(new Callable[Set[Itemset]] {
        //        def call(): Set[Itemset] = {
        //          run("resources/PAMPA_breastcancer_positive.txt", 7)
        //        }
        //      })
        //      ex.submit(new Callable[Set[Itemset]] {
        //        def call(): Set[Itemset] = {
        //          run("resources/PAMPA_breastcancer_positive.txt", 6)
        //        }
        //      })
        //      ex.submit(new Callable[Set[Itemset]] {
        //        def call(): Set[Itemset] = {
        //          run("resources/PAMPA_breastcancer_positive.txt", 5)
        //        }
        //      })
        //      ex.submit(new Callable[Set[Itemset]] {
        //        def call(): Set[Itemset] = {
        //          run("resources/PAMPA_breastcancer_positive.txt", 4)
        //        }
        //      })
        //      ex.submit(new Callable[Set[Itemset]] {
        //        def call(): Set[Itemset] = {
        //          run("resources/PAMPA_breastcancer_positive.txt", 3)
        //        }
        //      })
        //      ex.submit(new Callable[Set[Itemset]] {
        //        def call(): Set[Itemset] = {
        //          run("resources/PAMPA_synthetic_1000000_30_5.txt", 27)
        //        }
        //      })
        //      ex.submit(new Callable[Set[Itemset]] {
        //        def call(): Set[Itemset] = {
        //          run("resources/PAMPA_synthetic_1000000_30_5.txt", 21)
        //        }
        //      })
        //      ex.submit(new Callable[Set[Itemset]] {
        //        def call(): Set[Itemset] = {
        //          run("resources/PAMPA_synthetic_1000000_30_5.txt", 20)
        //        }
        //      })
        //      ex.submit(new Callable[Set[Itemset]] {
        //        def call(): Set[Itemset] = {
        //          run("resources/PAMPA_synthetic_1000000_30_5.txt", 19)
        //        }
        //      })
        //      ex.submit(new Callable[Set[Itemset]] {
        //        def call(): Set[Itemset] = {
        //          run("resources/PAMPA_synthetic_1000000_30_5.txt", 18)
        //        }
        //      })
        //      ex.submit(new Callable[Set[Itemset]] {
        //        def call(): Set[Itemset] = {
        //          run("resources/PAMPA_synthetic_1000000_30_5.txt", 17)
        //        }
        //      })
        //      ex.submit(new Callable[Set[Itemset]] {
        //        def call(): Set[Itemset] = {
        //          run("resources/PAMPA_synthetic_10000000_30_5.txt", 25)
        //        }
        //      })
        //      ex.submit(new Callable[Set[Itemset]] {
        //        def call(): Set[Itemset] = {
        //          run("resources/PAMPA_synthetic_10000000_30_5.txt", 24)
        //        }
        //      })
        //      ex.submit(new Callable[Set[Itemset]] {
        //        def call(): Set[Itemset] = {
        //          run("resources/PAMPA_synthetic_10000000_30_5.txt", 23)
        //        }
        //      })
        //      ex.submit(new Callable[Set[Itemset]] {
        //        def call(): Set[Itemset] = {
        //          run("resources/PAMPA_synthetic_10000000_30_5.txt", 22)
        //        }
        //      })
        //      ex.submit(new Callable[Set[Itemset]] {
        //        def call(): Set[Itemset] = {
        //          run("resources/PAMPA_synthetic_10000000_30_5.txt", 21)
        //        }
        //      })
        //      ex.submit(new Callable[Set[Itemset]] {
        //        def call(): Set[Itemset] = {
        //          run("resources/PAMPA_synthetic_10000000_30_5.txt", 20)
        //        }
        //      })
        //    })
        //    ex.shutdown()
    }

    /**
     * A generic itemset, contains a set of item of any Type and a value for the support.
     *
     * @param items a set containing the items
     * @param sup   an integer whhich contains the support of those items.
     */
    class Itemset(val items: Set[Any], val sup: Int) {
        lazy val hash = Objects.hashCode(items)

        override def toString: String = s"{${items.toVector.sortBy(_.toString)}; $sup}"

        override def equals(obj: scala.Any): Boolean = obj match {
            case aItemset: Itemset => aItemset.items.equals(items)
            case _ => false
        }

        override def hashCode(): Int = hash
    }

    class TT(var d: Iterable[(Any, RoaringBitmap)], val X: RoaringBitmap, val deleted: Int) {
        lazy val Y: RoaringBitmap = if (d.isEmpty) RoaringBitmap.bitmapOf() else d.map(_._2).reduce(RoaringBitmap.and)
        lazy val support: Int = X.getCardinality + deleted
        lazy val FX = new Itemset(d.map(_._1).toSet, support + Y.getCardinality)
        lazy val data: Iterable[(Any, RoaringBitmap)] = if (Y.isEmpty) d else d.map(t => (t._1, RoaringBitmap.andNot(t._2, Y)))

        override def toString: String = s"{$X, $support}"
    }

    /**
     * Companion object for the itemset class(why don't ise a case class? maybe for the equals method?)
     */
    object Itemset {
        def apply(items: Set[Any], sup: Int): Itemset = new Itemset(items, sup)
    }

    object TT {
        def apply(ttX: TT, ri: Int, deleted: Int): TT = {
            require(!ttX.X.contains(ri))
            val X: RoaringBitmap = RoaringBitmap.add(ttX.X, ri, ri + 1)
            new TT(ttX.data.filter(r => r._2.contains(ri)).map(r => (r._1, RoaringBitmap.andNot(r._2, X))), X, ttX.deleted + deleted)
        }

        def apply(dataSet: Iterable[(Int, Set[Any])], ri: Int): TT = {
            new TT(
                dataSet
                    .flatMap({ case (id: Int, row: Set[Any]) => row.map({ item: Any => (item, id) }) })
                    .groupBy(_._1)
                    .map({ case (item: Any, tids: Iterable[(Any, Int)]) => (item, RoaringBitmap.bitmapOf(tids.map(_._2).toArray: _*)) })
                    .filter({ case (item: Any, tids: RoaringBitmap) => tids.contains(ri) })
                    .mapValues({ tids: RoaringBitmap => RoaringBitmap.bitmapOf(tids.toArray.filter({ tid: Int => tid > ri }): _*) }), RoaringBitmap.bitmapOf(ri), 0)
        }

        def apply(dataSet: RDD[(Long, Set[Any])], ri: Int): TT = {
            new TT(
                dataSet
                    .flatMap({ case (id: Long, row: Set[Any]) => row.map({ item: Any => (item, id.toInt) }) })
                    .groupBy(_._1)
                    .map({ case (item: Any, tids: Iterable[(Any, Int)]) => (item, RoaringBitmap.bitmapOf(tids.map(_._2).toArray: _*)) })
                    .filter({ case (item: Any, tids: RoaringBitmap) => tids.contains(ri) })
                    .mapValues({ tids: RoaringBitmap => RoaringBitmap.bitmapOf(tids.toArray.filter({ tid: Int => tid > ri }): _*) })
                    .collect(), RoaringBitmap.bitmapOf(ri), 0)
        }
    }

}
