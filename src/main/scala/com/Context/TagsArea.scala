package com.Context

import com.LogsUtils.Tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 地域标签
  */
object TagsArea extends Tags{
  /**
    * 标签接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]
    val pro = row.getAs[String]("provincename")
    val cityname = row.getAs[String]("cityname")
    //判断省市是否为空
    if(StringUtils.isNoneBlank(pro)) {
      list:+= ("ZP" + pro, 1)
    }
    if(StringUtils.isNoneBlank(cityname)) {
      list:+= ("ZC" + cityname, 1)
    }
    list
  }
}
