package it.unibo.big

import it.unibo.big.TemporalScale.AbsoluteScale
import it.unibo.big.Utils.startSparkSession
import it.unibo.big.temporal.TemporalCellBuilder.{computeLatitudeQuery, computeLongitudeQuery, computeTimeBin}
import org.apache.spark.sql.SaveMode

import java.io._
import java.util.Scanner

/** This is the main class of the project */
object Query {

    /**
     * Main of the whole application.
     *
     * @param args arguments
     */
    def main(args: Array[String]): Unit = {
        val conf = new Conf(args)
        run(
            inTable = conf.tbl.getOrElse("inTable").replace("trajectory.", ""),
            minsize = conf.minsize.getOrElse(1000),
            minsup = conf.minsup.getOrElse(20),
            bin_s = conf.bins.getOrElse(20),
            bin_t = conf.bint.getOrElse(5),
            eps_t = conf.epst.getOrElse(Double.PositiveInfinity),
            timescale = TemporalScale(conf.timescale.getOrElse(AbsoluteScale.value)),
            euclidean = conf.euclidean.getOrElse(true),
            querytype = conf.querytype.getOrElse("export"),
            limit = conf.limit.getOrElse(1000000)
        )
    }

    /**
     * @param inTable Input table
     * @param minsize Minimum size of the co-movement patterns
     * @param minsup  Minimum support of the co-movement patterns (i.e., how long trajectories moved together)
     * @param bin_s   bin_s \in N (1, ..., n). Multiply cell size 123m x bin_s
     */
    def run(inTable: String, minsize: Int, minsup: Int, bin_s: Int, timescale: TemporalScale, bin_t: Int, eps_t: Double, euclidean: Boolean, querytype: String, limit: Int): Unit = {
        querytype match {
            case "export" => exportData(inTable, minsize, minsup, bin_s, timescale, bin_t, eps_t, euclidean, limit)
        }
    }

    /** Export original and binned data from the valid patterns */
    def exportData(inTable: String, minsize: Int, minsup: Int, bin_s: Int, timescale: TemporalScale, bin_t: Int, eps_t: Double, euclidean: Boolean, lmt: Int): Unit = {
        val sparkSession = startSparkSession(appName = "QueryCTM", master = "yarn")
        val epst = if (eps_t == Int.MaxValue) "Infinity" else eps_t.toString
        val sql = //  order by 2 desc limit 2
            s"""select i.itemsetid, t.itemid, t.tid, t.latitude as bin_latitude, t.longitude as bin_longitude, t.time_bucket as bin_timestamp, u.tileid as in_support, s.userid, s.trajectoryid, s.`timestamp`, s.latitude, s.longitude
               |from (select itemsetid, support from ctm.CTM__tbl_${inTable}__lmt_${lmt}__size_${minsize}__sup_${minsup}__bins_${bin_s}__ts_${timescale.value}__bint_${bin_t}__epss_Infinity__epst_${epst}__freq_1__sthr_1000000__summary order by support desc limit 10) a
               |     join ctm.CTM__tbl_${inTable}__lmt_${lmt}__size_${minsize}__sup_${minsup}__bins_${bin_s}__ts_${timescale.value}__bint_${bin_t}__epss_Infinity__epst_${epst}__freq_1__sthr_1000000__itemset i on (a.itemsetid = i.itemsetid)
               |     join ctm.tmp_transactiontable__tbl_${inTable}__lmt_${lmt}__size_${minsize}__sup_${minsup}__bins_${bin_s}__ts_${timescale.value}__bint_${bin_t} t on (t.itemid = i.itemid)
               |     left join ctm.CTM__tbl_${inTable}__lmt_${lmt}__size_${minsize}__sup_${minsup}__bins_${bin_s}__ts_${timescale.value}__bint_${bin_t}__epss_Infinity__epst_${epst}__freq_1__sthr_1000000__support u on (t.tid = u.tileid and i.itemsetid = u.itemsetid)
               |     join trajectory.${inTable} s on (t.userid = s.userid and t.trajectoryid = s.trajectoryid
               |        and ${computeTimeBin(timescale, bin_t, column = "s.`timestamp`")} = t.time_bucket
               |        and ${computeLatitudeQuery(euclidean, bin_s, "s.latitude")} = t.latitude
               |        and ${computeLongitudeQuery(euclidean, bin_s, "s.longitude")} = t.longitude)""".stripMargin
        var tablename = s"join__${inTable}__${minsize}__${minsup}__${bin_s}__${timescale.value}__${bin_t}"

        println(s"SQL:\n$sql")
        sparkSession.sql("use ctm")
        sparkSession.sql(sql).write.mode(SaveMode.Overwrite).saveAsTable(tablename)

        val pw = new PrintWriter(new File("saveQuery.sh"))
        var hivequery = s"select itemsetid, itemid, tid, userid, trajectoryid, `timestamp`, latitude, longitude, bin_latitude, bin_longitude, bin_timestamp, in_support from $tablename"
        var hiveCommand = s"hive -e 'set hive.cli.print.header=true; use ctm; $hivequery' | sed 's/[\\t]/,/g'  > $tablename.csv"
        println(hiveCommand)
        pw.write(s"$hiveCommand\n")

        // sparkSession.sql(s"select itemid, bin_latitude, bin_longitude, bin_timestamp from $tablename").rdd.map(r => (r.get(3), (r.get(0), r.get(1), r.get(2))))
        hivequery = s"select distinct itemid, bin_latitude, bin_longitude, bin_timestamp from $tablename"
        hiveCommand = s"hive -e 'set hive.cli.print.header=true; use ctm; $hivequery' > $tablename.tsv"
        var hdfsCommand = s"hdfs dfs -put $tablename.tsv /user/mfrancia/spare/input"
        println(hiveCommand)
        pw.write(s"$hiveCommand\n")
        println(hdfsCommand)
        pw.write(s"$hdfsCommand\n")

        tablename = s"tmp_transactiontable__tbl_${inTable}__lmt_${lmt}__size_${minsize}__sup_${minsup}__bins_${bin_s}__ts_${timescale.value}__bint_${bin_t}"
        hivequery = s"select itemid, latitude, longitude, time_bucket from $tablename"
        hiveCommand = s"hive -e 'set hive.cli.print.header=true; use ctm; $hivequery' > $tablename.tsv"
        hdfsCommand = s"hdfs dfs -put $tablename.tsv /user/mfrancia/spare/input"
        println(hiveCommand)
        pw.write(s"$hiveCommand\n")
        println(hdfsCommand)
        pw.write(s"$hdfsCommand\n")

        // df.repartition(1).write.option("sep", ",").option("header", "true").option("escape", "\"").option("quoteAll", "true").csv("file:////home/mfrancia/ctm/output/")
        pw.close()

        execCmd("chmod u+x saveQuery.sh")
        execCmd("./saveQuery.sh")
    }

    @throws[java.io.IOException]
    def execCmd(cmd: String) = {
        val proc = Runtime.getRuntime.exec(cmd)
        proc.waitFor()
        val is = proc.getInputStream
        val s = new Scanner(is).useDelimiter("\\A")
        var `val` = ""
        if (s.hasNext) `val` = s.next
        else `val` = ""
        println(`val`)
    }
}