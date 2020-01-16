package com.wbbigdata.test

import constant.Constants
import utils.{StringUtils, ValidUtils}

object TestDemo {

  def main(args: Array[String]): Unit = {

    val line = "sessionid=63746095035e4c89a5b5bd0bd82e9988|" +
      "searchwords=华为手机,小龙虾,保温杯,卫生纸,苹果耳机,RedmiBook,MacBook,小米笔记本,吸尘器|" +
      "clickCategoryIds=49,67,31,42,19,95,22,27,86,24,9,12,69,30,20,30,96|" +
      "visitLength=3487|" +
      "stepLength=84|" +
      "startTime=2020-01-13 02:00:06|" +
      "age=18|" +
      "professional=professional87|" +
      "sex=female|" +
      "city=city94"

    val filterInfo = "startAge=20|endAge=50|professionals=professional87|cities=|sex=|keywords=|categoryIds="

    val fieldValue = StringUtils.getFieldFromConcatString(line, "\\|", "searchwords")
    val fieldValue2 = StringUtils.getFieldFromConcatString(filterInfo, "\\|", "professionals")

    println(fieldValue)
    println(fieldValue2)

    val bool = ValidUtils.in(line,"professional",filterInfo,Constants.PARAM_PROFESSIONALS)
    println(bool)
  }

}
