package com.Context

import com.LogsUtils.TagsUtils
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession

/**
  * 创建上下文标签
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    //判断路径参数
    if(args.length != 5) {
      println("参数错误，程序退出")
      sys.exit()
    }
    val Array(inputPath, outputPath, dirPath, stopWordPath,day) = args
    //初始化，并设置序列化机制
    val spark = SparkSession.builder().appName("LocationCount").master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer").getOrCreate()
    val df = spark.read.parquet(inputPath)

    //加载字典文件，进行广播
    val dirfile = spark.sparkContext.textFile(dirPath)
    val map = dirfile.filter(_.split("\t",-1).length>=5).map(t => (t.split("\t", -1)(4), t.split("\t", -1)(1))).collectAsMap()
    val broadcast = spark.sparkContext.broadcast(map)

    //加载停用词库，进行广播
    val stopkw = spark.sparkContext.textFile(stopWordPath)
    val arr = stopkw.collect()
    val broadKeyWord = spark.sparkContext.broadcast(arr)

    //加载配置文件
    val load = ConfigFactory.load()
    val tableName = load.getString("hbase.table.Name")
    val zkHost = load.getString("hbase.zookeeper.host")

    //创建hadoop任务配置项
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("hbase.zookeeper.quorum", zkHost)

    //创建conn连接
    val hbConn = ConnectionFactory.createConnection(hadoopConf)
    val hbAdmin = hbConn.getAdmin
    if (!hbAdmin.tableExists(TableName.valueOf(tableName))) {
      println("表可用")
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
      val columnDescriptor = new HColumnDescriptor("tags")
      tableDescriptor.addFamily(columnDescriptor)
      hbAdmin.createTable(tableDescriptor)
      hbAdmin.close()
      hbConn.close()
    }
    //加载hbase相关属性配置
    val jobConf = new JobConf(hadoopConf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,tableName)

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

      //返回所有标签
      (userID,ad++app++channel++device++kws++area)
    }).reduceByKey((list1,list2) => (list1:::list2).groupBy(_._1).mapValues(_.foldLeft(0)(_+_._2)).toList)
      .map{
        case (userid,userTags) => {
          val put = new Put(Bytes.toBytes(userid))
           val tags = userTags.map(t => t._1 + ":" + t._2).mkString(",")
          put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(day),Bytes.toBytes(tags))
          (new ImmutableBytesWritable(), put)
        }
      }
      .saveAsHadoopDataset(jobConf)
    spark.close()
  }
}
