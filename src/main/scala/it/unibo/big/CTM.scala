package it.unibo.big

import it.unibo.big.TemporalScale.NoScale
import it.unibo.big.Utils._
import it.unibo.big.temporal.TemporalCellBuilder
import it.unibo.big.temporal.TemporalCellBuilder._
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.roaringbitmap.RoaringBitmap
import org.rogach.scallop.ScallopConf

class CTM {}

/** This is the main class of the project */
object CTM {
    val linesToPrint = 20
    var debug: Boolean = _
    var returnResult: Boolean = _
    var partitions: Int = _
    var inTable: String = _
    var conf: String = _
    var summaryTable: String = _
    var itemsetTable: String = _
    var supportTable: String = _
    var outTable2: String = _
    var transactionTable: String = _
    var cellToIDTable: String = _
    var neighborhoodTable: String = _
    var storage_thr: Int = _
    var repfreq: Int = _
    var limit: Int = _
    var nexecutors: Int = _
    var ncores: Int = _
    var maxram: String = _
    var timeScale: TemporalScale = _
    var bin_t: Int = _
    var eps_t: Double = _
    var bin_s: Int = _
    var eps_s: Double = _
    var euclidean: Boolean = _
    private val L = Logger.getLogger(classOf[CTM])

    // scalastyle:off
    /**
     * Run CTM
     *
     * @param inTable      Input table
     * @param debugData    (debug purpose) If non empty, use this instead of `inTable` (for debug purpose, see `TemporalTrajectoryFlowTest.scala`)
     * @param minsize      Minimum size of the co-movement patterns
     * @param minsup       Minimum support of the co-movement patterns (i.e., how long trajectories moved together)
     * @param storage_thr  Store co-movement patterns if they exceed this threshold.
     * @param repfreq      repfreq \in N. Repartition frequency (how frequently co-movement are repartitioned)
     * @param limit        Limit the size of the input dataset
     * @param nexecutors   Number of executors
     * @param ncores       Number of cores
     * @param maxram       Available RAM
     * @param timeScale    {"daily", "weekly", "absolute", "notime"}. Daily = Day hour (0, ..., 23). Weekly = Weekday (1, ..., 7).
     * @param bin_t        bin_t \in N (0, 1, ..., n). Size of the temporal quanta (expressed as a multiplier of timeBucketUnit). If 0, consider only a single bucket
     * @param eps_t        eps_t \in N (1, ..., n). Allow eps_t temporal neighbors
     * @param bin_s        bin_s \in N (1, ..., n). Multiply cell size 123m x bin_s
     * @param eps_s        eps_s \in N (1, ..., n). Allow eps_s spatial neighbors
     * @param returnResult (debug purpose) If True, return the results as a centralized array. Otherwise, store it to hive table
     * @param droptable    Whether the support table should be dropped and created brand new
     * @param euclidean    Whether the coordinates in the dataset are Polar or Euclidean
     * @return If `returnResult`, return the results as a centralized array. Otherwise, returns an empty array
     */
    def run(inTable: String = "foo" + System.currentTimeMillis(), minsize: Int, minsup: Int, platoon: Boolean = false,
            storage_thr: Int = STORAGE_THR, repfreq: Int = 1, limit: Int = Int.MaxValue, // EFFICIENCY PARAMETERS
            nexecutors: Int = NEXECUTORS, ncores: Int = NCORES, maxram: String = MAXRAM, // SPARK CONFIGURATION
            timeScale: TemporalScale, bin_t: Int = 1, eps_t: Double = Double.PositiveInfinity,
            bin_s: Int, eps_s: Double = Double.PositiveInfinity, // EFFECTIVENESS PARAMETERS
            debugData: Seq[(Tid, Vector[Itemid])] = Seq(), neighs: Map[Tid, RoaringBitmap] = Map(), spark: Option[SparkSession] = None, // INPUTS
            returnResult: Boolean = false, droptable: Boolean = false, euclidean: Boolean = false): (Long, Array[(RoaringBitmap, Int, Int)], Long) = {
        this.inTable = inTable
        this.debug = debugData.nonEmpty
        this.returnResult = returnResult
        this.partitions = nexecutors * ncores * 3
        this.storage_thr = storage_thr
        this.repfreq = repfreq
        this.limit = limit
        this.nexecutors = nexecutors
        this.ncores = ncores
        this.maxram = maxram
        this.timeScale = timeScale
        this.bin_t = bin_t
        this.eps_t = eps_t
        this.bin_s = bin_s
        this.eps_s = eps_s
        this.euclidean = euclidean

        val temporaryTableName: String = // Define the generic name of the support table, based on the relevant parameters for cell creation.
            s"-tbl_${inTable.substring(Math.max(0, inTable.indexOf(".") + 1), inTable.length)}" +
                // {inTable.replace("trajectory.", "")}" +
                s"-lmt_${if (limit == Int.MaxValue) "Infinity" else limit}" +
                s"-size_$minsize" +
                s"-sup_$minsup" +
                s"-bins_$bin_s" +
                s"-ts_${timeScale.value}" +
                s"-bint_$bin_t"
        conf = // Define the generic name for the run, including temporary table name
            "CTM" + temporaryTableName +
                s"-epss_${if (eps_s.isInfinite) eps_s.toString else eps_s.toInt}" +
                s"-epst_${if (eps_t.isInfinite) eps_t.toString else eps_t.toInt}" +
                s"-freq_${repfreq}" +
                s"-sthr_${storage_thr}"
        summaryTable = conf.replace("-", "__") + "__summary"
        itemsetTable = conf.replace("-", "__") + "__itemset"
        supportTable = conf.replace("-", "__") + "__support"
        outTable2 = s"results/CTM_stats.csv"

        val sparkSession = spark.getOrElse(startSparkSession(conf, nexecutors, ncores, maxram, SPARK_SQL_SHUFFLE_PARTITIONS, "yarn"))
        var neighbors: Map[Tid, RoaringBitmap] = Map()
        val trans: RDD[(Tid, Itemid)] =
            if (debug) {
                transactionTable = inTable
                sparkSession.sparkContext.parallelize(debugData.flatMap({ case (cell: Tid, items: Vector[Itemid]) => items.map(item => (item, Array(cell))) }))
                    .reduceByKey(_ ++ _, partitions)
                    .flatMap(t => t._2.map(c => (c, t._1)))
                    .cache()
            } else {
                /* *************************************************************************************************************
                 * CONFIGURING TABLE NAMES
                 * ************************************************************************************************************/
                transactionTable = s"tmp_transactiontable$temporaryTableName".replace("-", "__") // Trajectories mapped to cells
                cellToIDTable = s"tmp_celltoid$temporaryTableName".replace("-", "__") // Cells with ids
                neighborhoodTable = s"tmp_neighborhood$temporaryTableName".replace("-", "__") // Neighborhoods
                val inputDFtable = s"tmp_abstract$temporaryTableName".replace("-", "__")
                L.info(s"""--- Writing to
                       |        $inputDFtable
                       |        $summaryTable
                       |        $itemsetTable
                       |        $supportTable
                       |        $transactionTable
                       |        $cellToIDTable
                       |        $neighborhoodTable
                       |        $outTable2""".stripMargin)

                /* *************************************************************************************************************
                 * Trajectory mapping: mapping trajectories to trajectory abstractions
                 * **************************************************************************************************************/
                if (droptable) {
                    L.debug("Dropping tables.")
                    sparkSession.sql(s"drop table if exists $DB_NAME.$inputDFtable")
                    sparkSession.sql(s"drop table if exists $DB_NAME.$transactionTable")
                    sparkSession.sql(s"drop table if exists $DB_NAME.$cellToIDTable")
                    sparkSession.sql(s"drop table if exists $DB_NAME.$neighborhoodTable")
                }
                sparkSession.sql(s"use $DB_NAME") // set the output trajectory database
                // the transaction table is only generated once, skip the generation if already generated
                if (!sparkSession.catalog.tableExists(DB_NAME, transactionTable)) {
                    // Create time bin from timestamp in a temporal table `inputDFtable`
                    L.debug(s"--- Getting data from $inTable...")
                    getData(sparkSession, inTable, inputDFtable, timeScale, bin_t, euclidean, bin_s)
                    sparkSession.sql(s"select * from $inputDFtable").show(linesToPrint)
                    // create trajectory abstractions from the `inputDFtable`, and store it to the `transactionTable` table
                    L.debug(s"--- Generating $transactionTable...")
                    mapToReferenceSystem(sparkSession, inputDFtable, transactionTable, minsize, minsup, limit)
                    sparkSession.sql(s"select * from $transactionTable").show(linesToPrint)
                    // create the quanta of the reference system from the `inputDFtable` temporal table, and store it to the `cellToIDTable`
                    getQuanta(sparkSession, transactionTable, cellToIDTable)
                    sparkSession.sql(s"select * from $cellToIDTable").show(linesToPrint)
                }
                if (!sparkSession.catalog.tableExists(DB_NAME, neighborhoodTable)) {
                    // create the table with all neighbors for each cell
                    if (!eps_s.isInfinite || !eps_t.isInfinite) {
                        L.debug(s"--- Generating $neighborhoodTable...")
                        createNeighborhood(sparkSession, cellToIDTable, neighborhoodTable, timeScale, euclidean)
                        sparkSession.sql(s"select * from $neighborhoodTable").show(linesToPrint)
                    }
                }
                require(sparkSession.catalog.tableExists(DB_NAME, transactionTable), s"$transactionTable does not exist")
                require(sparkSession.catalog.tableExists(DB_NAME, transactionTable), s"$cellToIDTable does not exist")
                require(sparkSession.catalog.tableExists(DB_NAME, transactionTable), s"$neighborhoodTable does not exist")

                val sql = s"select tid, itemid from $transactionTable"
                L.debug(s"--- Input: $sql")
                sparkSession
                    .sql(sql)
                    .rdd
                    .map(i => (i.get(1).asInstanceOf[Int], Array(i.get(0).asInstanceOf[Int])))
            }
                .reduceByKey(_ ++ _, partitions)
                .flatMap(t => t._2.map(c => (c, t._1)))
                .cache()

        if (debug) {
            neighbors = neighs
        } else {
            /* ***************************************************************************************************************
             * BROADCASTING NEIGHBORHOOD
             * Define a neighborhood for each trajectoryID and broadcast it (only if a neighborhood metrics is defined).
             * Carpenter by default would not include this, thanks to this the algorithm will be able to use bounds on time and space
             * ************************************************************************************************************ */
            neighbors =
                if (!eps_s.isInfinite || !eps_t.isInfinite) {
                    val spaceThreshold = if (eps_s.isInfinite) { None } else { Some(eps_s * bin_s * DEFAULT_CELL_SIDE) }
                    val timeThreshold = if (eps_t.isInfinite) { None } else { Some(eps_t) }
                    TemporalCellBuilder.broadcastNeighborhood(sparkSession, spaceThreshold, timeThreshold, neighborhoodTable)
                } else {
                    Map()
                }
        }
        val brdNeighborhood: Option[Broadcast[Map[Tid, RoaringBitmap]]] =
            if (neighbors.nonEmpty) {
                Some(sparkSession.sparkContext.broadcast(neighbors))
            } else {
                None
            }

        /** run the algorithm. */
        CTM2.CTM(sparkSession, trans, brdNeighborhood, minsup, minsize, platoon)
    }

    /**
     * Main of the whole application.
     *
     * @param args arguments
     */
    def main(args: Array[String]): Unit = {
        val conf = new Conf(args)
        val debug: Boolean = conf.debug()
        if (args.nonEmpty && !debug) {
            run(
                inTable = conf.tbl(),
                euclidean = conf.euclidean(),
                droptable = conf.droptable(),
                timeScale = TemporalScale(conf.timescale.getOrElse(NoScale.value)),
                bin_t = conf.bint.getOrElse(0),
                eps_t = conf.epst.getOrElse(Double.PositiveInfinity),
                eps_s = conf.epss.getOrElse(Double.PositiveInfinity),
                bin_s = conf.bins(),
                nexecutors = conf.nexecutors(),
                ncores = conf.ncores(),
                maxram = conf.maxram(),
                storage_thr = conf.storagethr.getOrElse(1000000),
                repfreq = conf.repfreq(),
                limit = conf.limit.getOrElse(conf.minsize() * 1000),
                minsize = conf.minsize(),
                minsup = conf.minsup(),
                platoon = conf.platoon(),
                returnResult = conf.returnresult()
            )
        }
    }
}

/**
 * Class to be used to parse CLI commands, the values declared inside specify name and type of the arguments to parse.
 *
 * @param arguments the programs arguments as an array of strings.
 */
class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val tbl = opt[String]()
    val limit = opt[Int]()
    val minsize = opt[Int]()
    val minsup = opt[Int]()
    val euclidean = opt[Boolean]()
    val platoon = opt[Boolean]()
    val bins = opt[Int]()
    val epss = opt[Double]()
    val repfreq = opt[Int]()
    val storagethr = opt[Int]()
    val nexecutors = opt[Int]()
    val ncores = opt[Int]()
    val maxram = opt[String]()
    val timescale = opt[String]()
    val unitt = opt[Int]()
    val bint = opt[Int]()
    val epst = opt[Double]()
    val debug = opt[Boolean](required = true)
    val droptable = opt[Boolean]()
    val returnresult = opt[Boolean]()
    val querytype = opt[String]()
    verify()
}