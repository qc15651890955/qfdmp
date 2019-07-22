package com.LogsUtils

/**
  * 类型转换工具
  *
  */
object StringUtil {
  //string 转 int
  def String2Int(string: String):Int={
    try{
      string.toInt
    }catch {
      case _ :Exception => 0
    }
  }

  //string 转 double
  def String2Double(string: String):Double={
    try {
      string.toDouble
    }catch {
      case _ : Exception => 0
    }
  }
}
