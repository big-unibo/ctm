package it.unibo.big

import org.apache.spark.sql.SparkSession

/** This is the main class of the project */
object Query {
    /**
     * @param inTable Input table
     * @param minsize Minimum size of the co-movement patterns
     * @param minsup  Minimum support of the co-movement patterns (i.e., how long trajectories moved together)
     * @param bin_s   bin_s \in N (1, ..., n). Multiply cell size 123m x bin_s
     */
    def run(inTable: String, minsize: Int, minsup: Int, bin_s: Int): Unit = {
        val sparkSession =
            SparkSession.builder()
                .appName("Query trajectory")
                .master("yarn")
                .enableHiveSupport
                .getOrCreate

        val sql =
            s"""select i.itemsetid, t.itemid, t.tid, s.userid, s.trajectoryid, s.`timestamp`, s.latitude, s.longitude, t.latitude as bin_latitude, t.longitude as bin_longitude, t.time_bucket * 3600 as bin_timestamp, u.tileid as in_support
               |from    ctm.tmp_transactiontable__${inTable}__lmt_10000000__size_${minsize}__sup_${minsup}__bins_${bin_s}__ts_absolute__bint_1__unitt_3600 t
               |     join (select * from ctm.CTM__${inTable}__lmt_10000000__size_${minsize}__sup_${minsup}__bins_${bin_s}__ts_absolute__bint_1__unitt_3600__epss_Infinity__epst_Infinity__freq_1__sthr_1000000__itemset order by support desc limit 10) i on (t.itemid = i.itemid)
               |     join trajectory.${inTable} s on (t.userid = s.userid
               |         and t.trajectoryid = s.trajectoryid and cast(`timestamp` / 3600 as int) = t.time_bucket
               |         and round(round(s.latitude  / (11 * ${bin_s}), 4) * (11 * ${bin_s}), 4) = t.latitude
               |         and round(round(s.longitude / (15 * ${bin_s}), 4) * (15 * ${bin_s}), 4) = t.longitude)
               |     left join ctm.CTM__${inTable}__lmt_10000000__size_${minsize}__sup_${minsup}__bins_${bin_s}__ts_absolute__bint_1__unitt_3600__epss_Infinity__epst_Infinity__freq_1__sthr_1000000__support u on (t.tid = u.tileid)
               |""".stripMargin
        print(sql)
        sparkSession
            .sql(sql)
            .write
            .format("com.databricks.spark.csv")
            .save(s"$inTable-$minsize-$minsup-${bin_s}.csv")
    }

    /**
     * Main of the whole application.
     *
     * @param args arguments
     */
    def main(args: Array[String]): Unit = {
        val conf = new TemporalTrajectoryFlowConf(args)
        run(
            inTable = conf.tbl(),
            bin_s = conf.bins(),
            minsize = conf.minsize(),
            minsup = conf.minsup()
        )
    }
}