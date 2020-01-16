package com.wbbigdata

/**
  * 对clickCount、orderCount、payCount进行二次排序
  * @param clickCount
  * @param orderCount
  * @param payCount
  */
case class SorkKey(clickCount: Long, orderCount: Long, payCount: Long) extends Ordered[SorkKey]{

  //this.compare(that)
  override def compare(that: SorkKey) = {

    if(this.clickCount -that.clickCount != 0){
       (this.clickCount - that.clickCount).toInt
    }else if (this.orderCount - that.orderCount != 0) {
       (this.orderCount - that.orderCount).toInt
    } else  {
      (this.payCount - that.payCount).toInt
    }
  }
}
