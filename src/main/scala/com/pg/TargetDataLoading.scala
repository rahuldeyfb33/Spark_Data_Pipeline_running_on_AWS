package com.pg

import com.pg.utils.{Constants, Utilities}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.JavaConversions._
import org.apache.spark.sql.types._

object TargetDataLoading {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.master("local[*]").appName("Pampers-Datamart").getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)

    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val redshiftConfig = rootConfig.getConfig("redshift_conf")
    val s3Config = rootConfig.getConfig("s3_conf")

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))

    sparkSession.udf.register("DataMart.FN_UUID", () => java.util.UUID.randomUUID().toString())

    val target_list = rootConfig.getStringList("target_list").toList

    for (tgt <- target_list) {
      val tgtConfig = rootConfig.getConfig(tgt)
      val jdbcUrl = Constants.getRedshiftJdbcUrl(redshiftConfig)
      val s3_bucket = s3Config.getString("s3_bucket")
      val srcTables = tgtConfig.getStringList("sourceTable").toList

      tgt match {
        case "REGIS_DIM" =>
          println("CASE REGIS_DIM")
          println("Reading table from staging " + tgtConfig.getStringList("sourceTable")(0))
          for(src <- srcTables) {
            val srcDf = Utilities.readFromRedshift(sparkSession, redshiftConfig, src, s3_bucket)
            println("reading from redshift Completed   <<<<<<<<<")
            srcDf.show(3)
            srcDf.createOrReplaceTempView(src.replace(".", "_"))
          }
          val regisDim = sparkSession.sql(tgtConfig.getString("loadingQuery")).coalesce(1)
            regisDim.show()
          Utilities.writeToRedshift(regisDim, redshiftConfig, s3_bucket, tgtConfig.getString("tableName"))

        case "CHILD_DIM" =>
          println("CASE CHILD_DIM")
          println("Reading table from staging " + tgtConfig.getStringList("sourceTable")(0))
          for(src <- srcTables) {
            val srcDf = Utilities.readFromRedshift(sparkSession, redshiftConfig, src, s3_bucket)
            srcDf.show(3)
            srcDf.createOrReplaceTempView(src.replace(".", "_"))
          }
          val childDF = sparkSession.sql(tgtConfig.getString("loadingQuery")).coalesce(1)

          childDF.show(3)
          childDF.rdd.getNumPartitions
          Utilities.writeToRedshift(childDF, redshiftConfig, s3_bucket, tgtConfig.getString("tableName"))

        case "RTL_TXN_FACT" =>
          println("CASE RTL_TXN_FACT")
          println("Reading table from staging " + tgtConfig.getStringList("sourceTable")(0))
          for(src <- srcTables) {
            val srcDf = Utilities.readFromRedshift(sparkSession, redshiftConfig, src, s3_bucket)
            srcDf.show(3)
            srcDf.createOrReplaceTempView(src.replace(".", "_"))
          }
         // sparkSession.sql(tgtConfig.getString("loadingQuery")).show(3)
          //println(s"transformation "+  tgtConfig.getStringList("sourceTable")(0).replace(".", "_") + " completed")


          val factDim = sparkSession.sql(tgtConfig.getString("loadingQuery")).coalesce(1)
          factDim.show(4)
          Utilities.writeToRedshift(factDim, redshiftConfig, s3_bucket, tgtConfig.getString("tableName"))
          //println(s"transformation"+  tgtConfig.getStringList("sourceTable")(0).replace(".", "_") + " completed")
      }

    }
    sparkSession.stop()
    }
  }

