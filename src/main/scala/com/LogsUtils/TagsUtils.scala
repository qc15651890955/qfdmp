package com.LogsUtils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsUtils {
  // 过滤条件，唯一id只要有一个不为空即可
  val userIdOne =
    """
      |imei !='' or mac!='' or idfa != '' or openudid!='' or androidid!='' or
      |imeimd5 !='' or macmd5!='' or idfamd5 != '' or openudidmd5!='' or androididmd5!='' or
      |imeisha1 !='' or macsha1!='' or idfasha1 != '' or openudidsha1!='' or androididsha1!=''
    """.stripMargin

  //获取不为空的用户ID
  def getAllOneUserId(row: Row): String = {
    row match {
      case v if StringUtils.isNoneBlank(v.getAs[String]("imei")) => "IM: " + v.getAs[String]("imei")
      case v if StringUtils.isNoneBlank(v.getAs[String]("mac")) => "IM: " + v.getAs[String]("mac")
      case v if StringUtils.isNoneBlank(v.getAs[String]("idfa")) => "IM: "+v.getAs[String]("idfa")
      case v if StringUtils.isNoneBlank(v.getAs[String]("openudid")) => "IM: "+v.getAs[String]("openudid")
      case v if StringUtils.isNoneBlank(v.getAs[String]("androidid")) => "IM: "+v.getAs[String]("androidid")
      case v if StringUtils.isNoneBlank(v.getAs[String]("imeimd5")) => "IM: "+v.getAs[String]("imeimd5")
      case v if StringUtils.isNoneBlank(v.getAs[String]("macmd5")) => "IM: "+v.getAs[String]("macmd5")
      case v if StringUtils.isNoneBlank(v.getAs[String]("idfamd5")) => "IM: "+v.getAs[String]("idfamd5")
      case v if StringUtils.isNoneBlank(v.getAs[String]("openudidmd5")) => "IM: "+v.getAs[String]("openudidmd5")
      case v if StringUtils.isNoneBlank(v.getAs[String]("androididmd5")) => "IM: "+v.getAs[String]("androididmd5")
      case v if StringUtils.isNoneBlank(v.getAs[String]("imeisha1")) => "IM: "+v.getAs[String]("imeisha1")
      case v if StringUtils.isNoneBlank(v.getAs[String]("macsha1")) => "IM: "+v.getAs[String]("macsha1")
      case v if StringUtils.isNoneBlank(v.getAs[String]("idfasha1")) => "IM: "+v.getAs[String]("idfasha1")
      case v if StringUtils.isNoneBlank(v.getAs[String]("openudidsha1")) => "IM: "+v.getAs[String]("openudidsha1")
      case v if StringUtils.isNoneBlank(v.getAs[String]("androididsha1")) => "IM: "+v.getAs[String]("androididsha1")
    }
  }

}
