package com.APP

import com.AppCountUtils.AppUtils
import org.apache.spark.sql.SparkSession

object OperatorCount {
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
    //根据需求对关联字段进行处理
    df.rdd.map(row => {
      //获取省，市信息，作为key
      val ispid = row.getAs[Int]("ispid")
      val ispname = row.getAs[String]("ispname")
      //获取REQUESTMODE，PROCESSNODE，ISEFFECTIVE，ISBILLING，ISBID，ISWIN，ADORDEERID作为value
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
      ((ispid,ispname),requestList++appadList++recountList)
    }).reduceByKey((list1,list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    }).saveAsTextFile(outputPath)
  }

}
