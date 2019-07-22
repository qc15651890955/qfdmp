package com.Rpt

import org.apache.spark.sql.SparkSession

object ProCityRptJSON {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("参数错误，程序退出")
      sys.exit()
    }
    val Array(inputPath,outputPath) = args
    val spark = SparkSession.builder().config("spark.serializer","org.apache.spark.serializer.KryoSerializer").appName("ProCityRptJSON").master("local[*]").getOrCreate()
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
    val df = spark.read.parquet(inputPath)
    df.createTempView("log")
    val df1 = spark.sql("select count(*) ct,provincename,cityname from log group by provincename,cityname order by count(*) desc")
    df1.write.partitionBy("provincename","cityname").json(outputPath)
    spark.close()
  }


}
