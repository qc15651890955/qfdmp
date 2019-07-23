package com.APP

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

object LocationCountSQL {
  def main(args: Array[String]): Unit = {
    //判断路径参数
    if(args.length != 2) {
      println("参数错误，程序退出")
      sys.exit()
    }
    val Array(inputPath, outputPath) = args
    //初始化，并设置序列化机制
    val spark = SparkSession.builder().appName("LocationCount").master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer").getOrCreate()
    //设置压缩模式
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
    //读取文件
    val df = spark.read.parquet(inputPath)
    df.createTempView("location")
    val result = spark.sql(
      """
select
provincename,
cityname,
sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) prime_res,
sum(case when requestmode=1 and processnode>=2 then 1 else 0 end) valid_res,
sum(case when requestmode=1 and processnode=3 then 1 else 0 end) ad_res,
sum(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end) in_bid,
sum(case when iseffective=1 and isbilling=1 and isbid=1 and iswin=1 and adorderid!=0 then 1 else 0 end) success_bid,
sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) shows,
sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) clint,
sum(case when iseffective=1 and isbilling=1 and isbid=1 and iswin=1 then winprice/1000 else 0 end) dsp_spend,
sum(case when iseffective=1 and isbilling=1 and isbid=1 and iswin=1 then adpayment/1000 else 0 end) dsp_cost
from location
group by provincename,cityname
      """.stripMargin)
    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","123456")
    result.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/dmps?characterEncoding=utf-8","rpt_location_count",properties)
  }

}
