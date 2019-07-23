package com.APP

import com.AppCountUtils.AppUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession


object AppCount {
  def main(args: Array[String]): Unit = {
    if(args.length != 3) {
      println("参数错误，程序退出")
      sys.exit()
    }
    val Array(inputPath, outputPath,dirPath) = args

    val spark = SparkSession.builder().appName("AppCount").master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer").getOrCreate()

    spark.sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")

    val df = spark.read.parquet(inputPath)
    //读取字典文件，并广播出去
    val dirfile = spark.sparkContext.textFile(dirPath)
    val map = dirfile.map(_.split("\t",-1)).filter(_.length >= 5).map(arr => (arr(4),arr(1))).collect().toMap
    val dirBroad = spark.sparkContext.broadcast(map)
    df.rdd.map(row => {
      // 通过广播变量进行判断取值
        var appname = row.getAs[String]("appname")
        if(!StringUtils.isNoneBlank(appname)) {
          appname = dirBroad.value.getOrElse(row.getAs[String]("appid"),"UNKNOW")
        }
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      //调用业务方法
      val requestList = AppUtils.Request(requestmode,processnode)
      val appadList = AppUtils.Appad(iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment)
      val recountList = AppUtils.ReCount(requestmode,iseffective)
      (appname,requestList++appadList++recountList)
    }).reduceByKey((list1,list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    }).coalesce(10).saveAsTextFile(outputPath)
  }
}
