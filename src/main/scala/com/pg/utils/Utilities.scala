package com.pg.utils

import com.typesafe.config.Config
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Utilities {
  def writeToRedshift(df: DataFrame, redshiftConfig: Config, s3Bucket: String, tgtTable: String): Unit = {
    println(s"Writing $tgtTable to redshift")
    val jdbcUrl = Constants.getRedshiftJdbcUrl(redshiftConfig)
    df.write
      .format("com.databricks.spark.redshift")
      .option("url", jdbcUrl)
      .option("tempdir", s"s3n://$s3Bucket/temp")
      .option("forward_spark_s3_credentials", "true")
      .option("csvnullstring","NULL")
      .option("extracopyoptions","EMPTYASNULL")
      .option("tempformat","CSV")
      .option("dbtable", tgtTable)
      .mode(SaveMode.Overwrite)
      .save()
    println("writing to redshift Completed   <<<<<<<<<")
  }

  def readFromRedshift (sparkSession: SparkSession, redshiftConfig: Config, srcTable: String,  s3Bucket: String): DataFrame = {
    val origDf = sparkSession.sqlContext.read
      .format("com.databricks.spark.redshift")
      .option("url", Constants.getRedshiftJdbcUrl(redshiftConfig))
      .option("tempdir", s"s3n://$s3Bucket/temp")
      .option("forward_spark_s3_credentials", "true")
      .option("dbtable", srcTable)
      .load()

    // origDf.rdd.getNumPartions

    sparkSession.sqlContext.read.schema(setNullableStateForAllStringColumns(origDf, true))
      .format("com.databricks.spark.redshift")
      .option("url", Constants.getRedshiftJdbcUrl(redshiftConfig))
      .option("tempdir", s"s3n://$s3Bucket/temp")
      .option("forward_spark_s3_credentials", "true")
      .option("dbtable", srcTable)
      .load()

  }

  def setNullableStateForAllStringColumns(df: DataFrame, nullable: Boolean) = {
    StructType(df.schema.map {
      case StructField( c, StringType, _, m) => StructField( c, StringType, nullable = nullable, m)
      case StructField( c, t, n, m) => StructField( c, t, n, m)
    })
  }

}
