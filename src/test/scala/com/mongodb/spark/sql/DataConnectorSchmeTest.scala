package com.mongodb.spark.sql

import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit


object DataConnectorSchmeTest extends Logging {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val Array(argSaveMode, updateTimeStart, updateTimeEnd, partDayHour, tableName) = (args ++ Array(null, null, null, null, null)).slice(0, 5)
    var saveMode = argSaveMode
    if (StringUtils.isNotBlank(argSaveMode)) {
      //default append
      saveMode = "append"
    }
    //条件处理好后，跑相关数据
    val calendar = Calendar.getInstance()
    val dateFormate: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.US)
    val yyyyMMddHHDF: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH", Locale.US)
    val sparkBuilder = SparkSession.builder()
    sparkBuilder.master("local[*]")
    val spark = sparkBuilder.appName(this.getClass.getName)
      .config("spark.mongodb.input.uri", "mongodb://candao_qc:candao_6.0_2018@10.200.102.127:27017/datacenter_release")
      .config("spark.mongodb.input.database", "datacenter_release")
      .getOrCreate()
    calendar.add(Calendar.HOUR, -1)
    val yyyy_MM_dd_HH = yyyyMMddHHDF.format(calendar.getTime)
    //指定数据分区，默认为跑数据的上一小时
    var yyyyMMddHH = yyyy_MM_dd_HH.replace("-", "").replace(" ", "")
    if (StringUtils.isNotBlank(partDayHour)) {
      yyyyMMddHH = partDayHour
    }
    println("run yyyy_MM_dd_HH=", yyyy_MM_dd_HH)
    handlerJob(spark, "", "account", yyyyMMddHH, saveMode)
    //    executorService.shutdown()
    spark.stop()
  }

  def handlerJob(spark: SparkSession, m_sql: String, tableName: String, yyyyMMddHH: String, saveMode: String): Unit = { //检测schema.().option("sampleSize", "1")
    var tableRdd = spark.emptyDataFrame
    var DFReader = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("collection", s"$tableName")
    //    val hivetable = spark.sql(s"select * from ods_mongodb_lxj.`$tableName` limit 1")
    //schema(hivetable.schema).
    //    hivetable.printSchema()
    tableRdd = DFReader.load().withColumn("dayhour", lit(yyyyMMddHH))
    tableRdd.printSchema()
    tableRdd.show()

    val colums = tableRdd.columns
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._

    tableRdd.schema.foreach(struct => {
      println("struct=>", struct)
      println("struct.name=>", struct.name)
      println("dataType=>", struct.dataType)
      println("simpleString=>", struct.dataType.simpleString)
      if (struct.dataType.simpleString.startsWith("array") || struct.dataType.simpleString.startsWith("map") ||
        struct.dataType.simpleString.startsWith("struct")) {
        tableRdd = tableRdd.withColumn(struct.name, col(struct.name).cast(StringType))
      }
    })

//    for (colName <- colums) {
//      tableRdd = tableRdd.withColumn(colName, col(colName).cast(StringType))
//    }
    println("xx")
    tableRdd.printSchema()
    tableRdd.show()


  }


}
