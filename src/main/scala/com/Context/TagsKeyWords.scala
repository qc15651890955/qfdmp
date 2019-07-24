package com.Context

import com.LogsUtils.Tags
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * 关键字标签
  */
object TagsKeyWords extends Tags{
  /**
    * 标签接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]
    val kws = args(1).asInstanceOf[Broadcast[Array[String]]]
    //获取关键字
    val kyeword = row.getAs[String]("keywords")
    kyeword.split("\\|").filter(words => {
      words.length >= 3 && words.length <= 8 && !kws.value.contains(words)
    }).foreach(f => {
      list:+= ("K" + f, 1)
    })
    list
  }
}
