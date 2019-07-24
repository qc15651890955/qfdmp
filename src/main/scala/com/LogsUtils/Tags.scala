package com.LogsUtils

trait Tags {
  /**
    * 标签接口
    */
  def makeTags(args: Any*): List[(String, Int)]
}
