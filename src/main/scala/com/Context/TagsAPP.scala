package com.Context

import com.LogsUtils.Tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * APP标签
  */
object TagsAPP extends Tags{
  /**
    * 标签接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val dir = args(1).asInstanceOf[Broadcast[collection.Map[String, String]]]
    //获取appname，appid
    val appname = row.getAs[String]("appname")
    val appid = row.getAs[String]("appid")
    //判断appname是否为空
    if(StringUtils.isNoneBlank(appname)){
      list:+= ("APP" + appname, 1)
    } else if(StringUtils.isNoneBlank(appid)) {
      list:+= ("APP" + dir.value.getOrElse(appid,appid), 1)
    }
    list
  }
}
