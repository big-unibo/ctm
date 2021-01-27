package it.unibo.big.temporal

import it.unibo.big.TemporalScale
import it.unibo.big.TemporalScale.{AbsoluteScale, DailyScale, NoScale, WeeklyScale}
import it.unibo.big.Utils._
import it.unibo.big.temporal.exception.InvalidTableSchemaException
import it.unibo.big.temporal.weekly.WeeklyHourTimeStamp
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.roaringbitmap.RoaringBitmap

object TemporalCellBuilder {
  /**
   * Get input data (with bucketed time and location).
   *
   * @param sparkSession the sparksession which will compute all the operations.
   * @param tableName    the table name to be processed.
   * @param timescale    time scale
   * @param bin_t        size of the bin time
   * @param unit_t       the unit size of the temporal cell, expressed in seconds.
   * @param euclidean    boolean to activate the metric based on euclidean distance.
   * @param bin_s        the border of the cell specified by input parameters.
   * @throws InvalidTableSchemaException if the table schema does not meet the requirements.
   */
  @throws(classOf[InvalidTableSchemaException])
  def getData(sparkSession: SparkSession, tableName: String, outTable: String, timescale: TemporalScale, bin_t: Int, unit_t: Int, euclidean: Boolean, bin_s: Int): Unit = {
    val binLatitude: String = computeLatitudeQuery(euclidean, bin_s)
    val binLongitude: String = computeLongitudeQuery(euclidean, bin_s)
    val conversionFunction: Long => Long = timescale match {
      case DailyScale => param => WeeklyHourTimeStamp(param).hourOfDay / bin_t
      case WeeklyScale => param => WeeklyHourTimeStamp(param).weeklyDay / bin_t
      case AbsoluteScale => param => param / bin_t
      case NoScale => _ => 0
    }
    val udfFunction = udf(conversionFunction)
    sparkSession.sql(
      s"""select distinct
         |       $USER_ID_FIELD, $TRAJECTORY_ID_FIELD, $binLatitude as $LATITUDE_FIELD_NAME, $binLongitude as $LONGITUDE_FIELD_NAME,
         |       CAST($TIMESTAMP_FIELD / ${if (timescale.value.equals(WeeklyScale.value) || timescale.value.equals(DailyScale.value)) 1 else unit_t} as BIGINT) as $BUCKET_TIMESTAMP_COLUMN_NAME
         |from $tableName
         |""".stripMargin
    )
      .withColumn(TIME_BUCKET_COLUMN_NAME, udfFunction(col(BUCKET_TIMESTAMP_COLUMN_NAME)))
      .createOrReplaceTempView(outTable)
    // TODO: BE AWARE, LIMIT CREATES A SINGLE PARTITION
  }

  private def computeLatitudeQuery(euclidean: Boolean, cellRound: Int): String =
    if (euclidean) {
      s"round(latitude / ${DEFAULT_CELL_SIDE * cellRound}, 0)"
    } else {
      s"round(round(latitude / ${11 * cellRound}, 4) * ${11 * cellRound}, 4)"
    }

  private def computeLongitudeQuery(euclidean: Boolean, cellRound: Int): String =
    if (euclidean) {
      s"round(longitude / ${DEFAULT_CELL_SIDE * cellRound}, 0)"
    } else {
      // s"cast(round(round(longitude / ${15 * cellRound}, 4) * ${15 * cellRound}, 4) * 10000 as int)"
      s"round(round(longitude / ${15 * cellRound}, 4) * ${15 * cellRound}, 4)"
    }

  /**
   * Get quanta.
   *
   * @param sparkSession     the spark session to compute all the operations.
   * @param transactionTable the input view, must have some specific columns inside.
   * @param outputTableName  the output table that will be stored on hive.
   * @return dataframe
   */
  def getQuanta(sparkSession: SparkSession, transactionTable: String, outputTableName: String): Long = {
    val storeHiveTbl =
      s"""create table if not exists $outputTableName as
         |    select distinct tid, $LATITUDE_FIELD_NAME, $LONGITUDE_FIELD_NAME, $TIME_BUCKET_COLUMN_NAME
         |    from $transactionTable""".stripMargin
    println(s"--- Generating `$outputTableName` table\n$storeHiveTbl")
    sparkSession.sql(storeHiveTbl).count()
  }

  /**
   * Create the transaction table, where each (userid, trajectoryid) is mapped to an itemid, and (lat, lon, timestamp) is mapped to tid
   *
   * @param sparkSession the spark session which will execute everything.
   * @param inTable      the name of the table containing the points and the temporal bucket.
   * @param minSize      minimum itemset size
   * @param minSup       minimum itemset support
   * @param limit        trajectories to retrieve
   */
  def mapToReferenceSystem(sparkSession: SparkSession, inTable: String, transactionTable: String, minSize: Int, minSup: Int, limit: Int): Unit = {
    import sparkSession.sqlContext.implicits._
    var rdd = sparkSession.sql(s"select $USER_ID_FIELD, $TRAJECTORY_ID_FIELD, $LATITUDE_FIELD_NAME, $LONGITUDE_FIELD_NAME, $TIME_BUCKET_COLUMN_NAME from $inTable")
      .rdd
      .map(row => ((row.get(0).asInstanceOf[String], row.get(1).asInstanceOf[String]), Array((row.get(2).asInstanceOf[Double], row.get(3).asInstanceOf[Double], row.get(4).asInstanceOf[Long]))))
      .reduceByKey(_ ++ _)

    var count = -1L
    var prevcount = -1L
    while (count < 0 || prevcount != count) {
      rdd = rdd
        .filter({ case ((userid: String, trajectoryid: String), locations: Array[(Double, Double, Long)]) => locations.length >= minSup })
        .flatMap({ case ((userid: String, trajectoryid: String), locations: Array[(Double, Double, Long)]) =>
          locations.map({ case (latitude: Double, longitude: Double, timestamp: Long) => ((latitude, longitude, timestamp), Array((userid, trajectoryid))) })
        })
        .reduceByKey(_ ++ _)
        .filter({ case ((latitude: Double, longitude: Double, timestamp: Long), trajectories: Array[(String, String)]) => trajectories.length >= minSize })
        .flatMap({ case ((latitude: Double, longitude: Double, timestamp: Long), trajectories: Array[(String, String)]) =>
          trajectories.map({ case (userid, trajectoryid) => ((userid, trajectoryid), Array((latitude, longitude, timestamp))) })
        })
        .reduceByKey(_ ++ _)
        .cache()
      prevcount = count
      count = rdd.count()
      println(s"reducing trajectories... $count")
    }

    rdd
      .zipWithIndex()
      .filter(t => t._2 <= limit)
      .flatMap({ case (((userid: String, trajectoryid: String), locations: Array[(Double, Double, Long)]), itemid: Long) =>
        locations.map({ case (latitude: Double, longitude: Double, timestamp: Long) => ((latitude, longitude, timestamp), Array((itemid.toInt, userid, trajectoryid))) })
      })
      .reduceByKey(_ ++ _)
      .filter({ case ((latitude: Double, longitude: Double, timestamp: Long), trajectories: Array[(Int, String, String)]) => trajectories.length >= minSize })
      .sortByKey()
      .zipWithIndex()
      .flatMap({
        case (((latitude: Double, longitude: Double, timestamp: Long), trajectories: Array[(Int, String, String)]), tid: Long) =>
          trajectories.map({ case (itemid, userid, trajectoryid) =>
            require(tid.toInt >= 0, "tid is below 0")
            (tid.toInt, itemid, userid, trajectoryid, latitude, longitude, timestamp)
          })
      })
      .toDF("tid", "itemid", USER_ID_FIELD, TRAJECTORY_ID_FIELD, LATITUDE_FIELD_NAME, LONGITUDE_FIELD_NAME, TIME_BUCKET_COLUMN_NAME)
      .write.mode(SaveMode.ErrorIfExists)
      .saveAsTable(transactionTable)
  }

  /**
   * Create a neighborhood table using weekly distance metrics.
   *
   * @param spark             the spark session which will compute all the operations.
   * @param cellTableName     the cell table name.
   * @param neighborhoodTable the neighborhood table name.
   * @param temporalScale     the temporal scale that must be adopted.
   * @param euclidean         euclidean coordinates
   */
  def createNeighborhood(spark: SparkSession, cellTableName: String, neighborhoodTable: String, temporalScale: TemporalScale, euclidean: Boolean): Unit = {
    // ST_Distance(ST_Transform(ST_GeomFromWKT(concat('POINT(', t.longitude, ' ', t.latitude, ')')), \"epsg:4326\", \"epsg:3857\"), ST_Transform(ST_GeomFromWKT(concat('POINT(', n.longitude, ' ', n.latitude, ')')), \"epsg:4326\", \"epsg:3857\")) as $SPACE_DISTANCE_COLUMN_NAME,
    val query =
      s"""select t.tid, n.tid as neigh,
         |       t.$LATITUDE_FIELD_NAME as l1, t.$LONGITUDE_FIELD_NAME as l2, n.$LATITUDE_FIELD_NAME as l3, n.$LONGITUDE_FIELD_NAME as l4,
         |       t.$TIME_BUCKET_COLUMN_NAME as t1, n.$TIME_BUCKET_COLUMN_NAME as t2
         |from $cellTableName t, $cellTableName n
         |where t.tid != n.tid
        """.stripMargin
    val computeTimeDistanceFunction: (Int, Int) => Int = temporalScale match {
      case DailyScale => (t1, t2) => WeeklyHourTimeStamp.computeDailyDistance(t1, t2)
      case WeeklyScale => (t1, t2) => WeeklyHourTimeStamp.computeWeeklyDistance(t1, t2)
      case AbsoluteScale => (t1, t2) => Math.abs(t1 - t2)
      case NoScale => (_, _) => 0
      case _ => throw new IllegalArgumentException(s"Cannot compute neighborhood for $temporalScale")
    }
    val computeTimeDistanceUDF = udf(computeTimeDistanceFunction)
    val computeSpaceDistanceUDF = euclidean match {
      case false => udf((l1: Double, l2: Double, l3: Double, l4: Double) => (Distances.haversine(l1, l2, l3, l4) * 1000).toInt)
      case true => udf((l1: Double, l2: Double, l3: Double, l4: Double) => Distances.euclideanDistance(l1, l2, l3, l4).toInt)
    }
    spark
      .sql(query)
      .withColumn(TIME_DISTANCE_COLUMN_NAME, computeTimeDistanceUDF(col("t1"), col("t2")))
      .withColumn(SPACE_DISTANCE_COLUMN_NAME, computeSpaceDistanceUDF(col("l1"), col("l2"), col("l3"), col("l4")))
      .write.mode(SaveMode.ErrorIfExists)
      .saveAsTable(neighborhoodTable)
  }

  /**
   * Create and broadcast the neighbourhood of each cell.
   *
   * @param spark              the spark session for the querying operations.
   * @param epss               an optional containing the spatial threshold, if specified.
   * @param epst               an optional containing the temporal threshold, if specified.
   * @param neighbourhoodTable the neighbourhood table name.
   * @return a cell-cell map containing for each cell its neighbourhood, an empty object if no spatio-temporal bound is specified.
   */
  def broadcastNeighborhood(spark: SparkSession, epss: Option[Double], epst: Option[Double], neighbourhoodTable: String): Map[Tid, RoaringBitmap] = {
    val basicQuery = s"select tid, neigh from $neighbourhoodTable where "
    var finalQuery = basicQuery
    epss match {
      case Some(sThreshold) => finalQuery += s"$SPACE_DISTANCE_COLUMN_NAME <= $sThreshold"
      case _ =>
    }
    epst match {
      case Some(tThreshold) if epss.isDefined => finalQuery += s" and $TIME_DISTANCE_COLUMN_NAME <= $tThreshold"
      case Some(tThreshold) => finalQuery += s"$TIME_DISTANCE_COLUMN_NAME <= $tThreshold"
      case _ =>
    }
    if (!(finalQuery equals basicQuery)) {
      spark
        .sql(finalQuery)
        .rdd
        .map(r => (r.get(0).asInstanceOf[Int], RoaringBitmap.bitmapOf(r.get(1).asInstanceOf[Int])))
        .reduceByKey(RoaringBitmap.or)
        .collect()
        .toMap
    } else {
      Map()
    }
  }
}
