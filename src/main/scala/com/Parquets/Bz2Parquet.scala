package com.Parquets

import com.LogsUtils.{SchemaUtils, StringUtil}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}


object Bz2Parquet {
  def main(args: Array[String]): Unit = {
    //判断目录参数是否为空
    if(args.length != 2) {
      println("目录参数不正确，程序退出")
      sys.exit()
    }
    //创建数组存储输入输出目录
    val Array(inputPath,outputPath) = args
    //初始化sparksession，并设置序列化机制
    val spark = SparkSession.builder().master("local[*]").appName("Bz2Parquet").config("spark.serializer","org.apache.spark.serializer.KryoSerializer").getOrCreate()
    //设置spark sql的压缩方式，1.6版本默认不是snappy，2.0以后默认是
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
    //读取文件
    val files = spark.sparkContext.textFile(inputPath)
    //内部无法解析“，，，，，，”，会识别成一个元素，所以切割的时候需要进行处理
    //过滤，要保证字段大于85个，不然会报数组越界
    val rowRDD = files.map(t => t.split(",", -1)).filter(_.length >= 85).map(arr => {
      Row(
        arr(0),
        StringUtil.String2Int(arr(1)),
        StringUtil.String2Int(arr(2)),
        StringUtil.String2Int(arr(3)),
        StringUtil.String2Int(arr(4)),
        arr(5),
        arr(6),
        StringUtil.String2Int(arr(7)),
        StringUtil.String2Int(arr(8)),
        StringUtil.String2Double(arr(9)),
        StringUtil.String2Double(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        StringUtil.String2Int(arr(17)),
        arr(18),
        arr(19),
        StringUtil.String2Int(arr(20)),
        StringUtil.String2Int(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        StringUtil.String2Int(arr(26)),
        arr(27),
        StringUtil.String2Int(arr(28)),
        arr(29),
        StringUtil.String2Int(arr(30)),
        StringUtil.String2Int(arr(31)),
        StringUtil.String2Int(arr(32)),
        arr(33),
        StringUtil.String2Int(arr(34)),
        StringUtil.String2Int(arr(35)),
        StringUtil.String2Int(arr(36)),
        arr(37),
        StringUtil.String2Int(arr(38)),
        StringUtil.String2Int(arr(39)),
        StringUtil.String2Double(arr(40)),
        StringUtil.String2Double(arr(41)),
        StringUtil.String2Int(arr(42)),
        arr(43),
        StringUtil.String2Double(arr(44)),
        StringUtil.String2Double(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        StringUtil.String2Int(arr(57)),
        StringUtil.String2Double(arr(58)),
        StringUtil.String2Int(arr(59)),
        StringUtil.String2Int(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        StringUtil.String2Int(arr(73)),
        StringUtil.String2Double(arr(74)),
        StringUtil.String2Double(arr(75)),
        StringUtil.String2Double(arr(76)),
        StringUtil.String2Double(arr(77)),
        StringUtil.String2Double(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        StringUtil.String2Int(arr(84))
      )
    })
    val df = spark.createDataFrame(rowRDD,SchemaUtils.logStructType)
    //df.show(20)
    df.write.parquet(outputPath)
    spark.close()
  }
}
