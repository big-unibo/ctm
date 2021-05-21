package it.unibo.big

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.rogach.scallop.ScallopConf

/** Map table to Milan neighborhood and municipalities */
object MapToMilan {
  val NEXECUTORS = 10
  val NCORES = 3
  val MAXRAM = "12g"

  /**
   * @param inTable    input table
   * @param outTable   output table
   * @param limit      number of records
   * @param nexecutors executors
   * @param maxram     ram per executor
   * @param ncores     cores per executors
   */
  def run(inTable: String, outTable: String, limit: Int = Integer.MAX_VALUE, nexecutors: Int, maxram: String, ncores: Int): Unit = {
    val spark: SparkSession =
      SparkSession.builder()
        .appName("MapToMilan")
        .config("spark.serializer", classOf[KryoSerializer].getName)
        .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        .config("geospark.global.index", "true")
        .config("geospark.join.gridtype", "quadtree")
        .enableHiveSupport.getOrCreate
    GeoSparkSQLRegistrator.registerAll(spark)
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    spark.sql(s"""select userid, trajectoryid, latitude, longitude, timestamp
                  |from $inTable
                  |where latitude >= 45.42 and latitude <= 45.5 and longitude >= 9.04 and longitude <= 9.275
                  |${if (limit.isInfinite) { "" } else { s"limit $limit" }}
                  |""".stripMargin).write.mode(SaveMode.ErrorIfExists).saveAsTable(outTable)
    // spark.sql(
    //   s"""select userid, trajectoryid, latitude, longitude, timestamp
    //      |from $inTable ${if (limit.isInfinite) { "" } else { s"limit $limit" }}
    //      |""".stripMargin)
    //   .createOrReplaceTempView("view1")
    // spark.sql(
    //   s"""select userid, trajectoryid, latitude, longitude, timestamp, n._c4 as nil
    //      |from view1 v, trajectory.milan_neighborhoods n
    //      |where ST_contains(ST_point(cast(longitude as decimal(38,10)), cast(latitude as decimal(38,10))), ST_Polygon(ST_GeomFromWKT(rddshape)))
    //      |""".stripMargin)
    //   .createOrReplaceTempView("view2")
    // spark.sql(
    //   s"""select userid, trajectoryid, latitude, longitude, timestamp, nil, n.municipality
    //      |from view2 v, (select rddshape, row_number() over (order by rddshape) as municipality from trajectory.milan_municipi) n
    //      |where ST_contains(ST_point(cast(longitude as decimal(38,10)), cast(latitude as decimal(38,10))), ST_Polygon(ST_GeomFromWKT(rddshape)))
    //      |""".stripMargin)
    //   .write.mode(SaveMode.ErrorIfExists).saveAsTable(outTable)
    // val comunDF: DataFrame = spark.sql(s"select * from trajectory.milan_comuni")
    spark.close
  }

  /**
   * Main of the whole application.
   * @param args arguments
   */
  def main(args: Array[String]): Unit = {
    val conf = new MapToMilanConf(args)
    run(
      inTable = conf.in(),
      outTable = conf.out(),
      nexecutors = conf.nexecutors.getOrElse(NEXECUTORS),
      ncores = conf.ncores.getOrElse(NCORES),
      maxram = conf.maxram.getOrElse(MAXRAM),
      limit = conf.limit.getOrElse(Int.MaxValue)
    )
  }
}

/**
 * Parse CLI commands, the values declared inside specify name and type of the arguments to parse.
 * @param arguments the programs arguments as an array of strings.
 */
class MapToMilanConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val in = opt[String](required = true)
  val out = opt[String](required = true)
  val limit = opt[Int]()
  val nexecutors = opt[Int]()
  val ncores = opt[Int]()
  val maxram = opt[String]()
  verify()
}