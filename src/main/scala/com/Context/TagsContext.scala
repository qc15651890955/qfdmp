package com.Context

import com.LogsUtils.TagsUtils
import org.apache.spark.sql.SparkSession

/**
  * 创建上下文标签
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    //判断路径参数
    if(args.length != 4) {
      println("参数错误，程序退出")
      sys.exit()
    }
    val Array(inputPath, outputPath, dirPath, stopWordPath) = args
    //初始化，并设置序列化机制
    val spark = SparkSession.builder().appName("LocationCount").master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer").getOrCreate()
    val df = spark.read.parquet(inputPath)

    //加载字典文件，进行广播
    val dirfile = spark.sparkContext.textFile(dirPath)
    val map = dirfile.map(t => (t.split("\t", -1)(4), t.split("\t", -1)(1))).collectAsMap()
    val broadcast = spark.sparkContext.broadcast(map)

    //加载停用词库，进行广播
    val stopkw = spark.sparkContext.textFile(stopWordPath)
    val arr = stopkw.collect()
    val broadKeyWord = spark.sparkContext.broadcast(arr)

    df.filter(TagsUtils.userIdOne).rdd.map(row => {
      //根据每一条数据，打对应的标签
      //获取用户的id
      val userID = TagsUtils.getAllOneUserId(row)
      //广告类型标签
      val ad = TagsAD.makeTags(row)
      //app标签
      val app = TagsAPP.makeTags(row,broadcast)
      //渠道标签
      val channel = TagsChannel.makeTags(row)
      //设备标签
      val device = TagsDevice.makeTags(row)
      //关键字标签
      val kws = TagsKeyWords.makeTags(row,broadKeyWord)
      //地域标签
      val area = TagsArea.makeTags(row)
      //商圈标签



    })

  }
}
