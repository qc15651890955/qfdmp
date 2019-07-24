package com.Context

import com.LogsUtils.Tags
import org.apache.spark.sql.Row

/**
  * 渠道标签
  */
object TagsChannel extends Tags{
  /**
    * 标签接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]
    // 渠道标签
    val channel = row.getAs[Int]("adplatformproviderid")
    list:+=("CN"+channel,1)
    list
  }
}
