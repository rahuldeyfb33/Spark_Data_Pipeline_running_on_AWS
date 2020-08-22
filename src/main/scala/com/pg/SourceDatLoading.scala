package com.pg

import com.pg.utils.{Constants, Utilities}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._

object SourceDatLoading {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.master("local[*]").appName("Pampers-Datamart").getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)


    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val redshiftConfig = rootConfig.getConfig("redshift_conf")
    val s3Config = rootConfig.getConfig("s3_conf")
    val s3Bucket = s3Config.getString("s3_bucket")

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))

    val sourceList = rootConfig.getStringList("source_list").toList

    for(src <- sourceList) {
      val srcConfig = rootConfig.getConfig(src)
        src match {
          case "SB" =>
            println("CASE SB")
            val mysqlConfig = srcConfig.getConfig("mysql_conf")
            var jdbcParams = Map("url" -> getMysqlJdbcUrl(mysqlConfig),
              "lowerBound" -> "1",
              "upperBound" -> "100",
              "dbtable" -> srcConfig.getString("src_table"),
              "numPartitions" -> "2",
              "partitionColumn" -> "App_Transaction_Id",
              "user" -> mysqlConfig.getString("username"),
              "password" -> mysqlConfig.getString("password")
            )

            println("\nReading data from MySQL DB using SparkSession.read.format(),")
            val txnDF = sparkSession
              .read.format("jdbc")
              .option("driver", "com.mysql.cj.jdbc.Driver")
              .options(jdbcParams) // options can pass map
              .load()
              .withColumn("INS_TS", current_timestamp())
              .repartition(1)
            txnDF.show(3)

          //  println("Writing data to redshift using sparksession.write.load()")
            Utilities.writeToRedshift(txnDF, redshiftConfig, s3Bucket, srcConfig.getString("tgt_table"))

        case "OL" =>
          println("CASE OL")
          val sftpConfig = srcConfig.getConfig("sftp_conf")
          println("\nReading data from SFTP SERVER using SparkSession.read.format(),")
          val olTxnDf = sparkSession.read.
            format("com.springml.spark.sftp").
            option("host", sftpConfig.getString("hostname")).
            option("port", sftpConfig.getString("port")).
            option("username", sftpConfig.getString("username")).
            option("pem", sftpConfig.getString("pem")).
            option("fileType", "csv").
            option("delimiter", "|").
            load(s"${sftpConfig.getString("directory")}/${srcConfig.getString("src_file")}")
            .withColumn("INS_TS", current_timestamp())
            .repartition(1)
          println("reading from SFTP Completed   <<<<<<<<<")
          olTxnDf.show(3)

          Utilities.writeToRedshift(olTxnDf, redshiftConfig, s3Bucket, srcConfig.getString("tgt_table"))

          case "1CP" =>
            println("CASE 1CP")
          println("Reading CSV file from s3 ")
          val cutomerDf = sparkSession.read
            .option("mode", "DROPMALFORMED")
            .option("header", "true")
            .option("delimiter", "|")
            .option("inferSchema", "true")
            .csv(s"s3n://${Constants.S3_BUCKET}/${srcConfig.getString("src_file")}")
            .withColumn("INS_TS", current_timestamp())
            .repartition(1)
          println("reading from S3 Completed   <<<<<<<<<")
          cutomerDf.show(3)

          Utilities.writeToRedshift(cutomerDf, redshiftConfig, s3Bucket, srcConfig.getString("tgt_table"))
        }
      }


    sparkSession.stop()
  }
  // Creating Redshift JDBC URL
  def getMysqlJdbcUrl(mysqlConfig: Config): String = {
    val host = mysqlConfig.getString("hostname")
    val port = mysqlConfig.getString("port")
    val database = mysqlConfig.getString("database")
    s"jdbc:mysql://$host:$port/$database?autoReconnect=true&useSSL=false"
  }
}
