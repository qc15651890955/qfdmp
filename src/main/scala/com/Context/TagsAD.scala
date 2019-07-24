package com.Context

import com.LogsUtils.Tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  *广告标签
  */
object TagsAD extends Tags{
  /**
    * 标签接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    // 解析参数
    val row = args(0).asInstanceOf[Row]
    // 获取广告类型 为广告类型打上标签
    val adType = row.getAs[Int]("adspacetype")
    adType match {
      case v if v >9 => list:+=("LC"+v,1)
      case v if v <= 9 && v>0 => list:+=("LC0"+v,1)
    }
    // 为广告名称打上标签
    val adName = row.getAs[String]("adspacetypename")
    if(StringUtils.isNoneBlank(adName)){
      list:+=("LN"+adName,1)
    }
    list
  }
}
