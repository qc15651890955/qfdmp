package com.AppCountUtils

object AppUtils {
  // 处理原始、有效、广告请求方法
  def Request(requestmode:Int, processnode:Int):List[Double] = {
    if(requestmode == 1 && processnode == 1) {
      //第一个参数 原始请求数
      //第二个参数 有效请求数
      //第三个参数 广告请求数
      List[Double](1,0,0)
    } else if(requestmode == 1 && processnode == 2) {
      List[Double](1,1,0)
    } else if(requestmode == 1 && processnode == 3) {
      List[Double](1,1,1)
    } else {
      List[Double](0,0,0)
    }
  }

  // 处理参与竞价数 竞价成功数 广告成品 广告消费
  def Appad(iseffective:Int,isbilling:Int,isbid:Int,iswin:Int,adorderid:Int,winprice:Double,adpayment:Double):List[Double] = {
    if(iseffective == 1 && isbilling == 1 && isbid == 1) {
      if (iswin == 1 && adorderid != 0) {
        List[Double](1,1,winprice/1000,adpayment/1000)
      } else {
        List[Double](1,0,0,0)
      }
    } else {
      List[Double](0,0,0,0)
    }
  }

  // 处理点击量和展示量
  def ReCount(requestmode:Int,iseffective:Int):List[Double] = {
    if(requestmode == 2 && iseffective == 1) {
      List[Double](1,0)
    } else if(requestmode == 3 && iseffective == 1) {
      List[Double](0,1)
    } else {
      List[Double](0,0)
    }
  }
}
