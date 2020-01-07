package utils

import java.util

object StringUtils {

  def fullfill(value: Int): String ={

    value.toString
  }

  /**
    * 判断字符串是否为空
    * @param value
    * @return
    */
  def isNotEmpty(value: String): Boolean = {
    return value != null
  }

  /**
    * 去除逗号
    * @param value
    * @return
    */
  def trimComma(value: String) = {
    value.substring(0, value.length)
  }

  /**
    * 从拼接的字符串中获取对应的字段信息
    * 将UUID取出，作为将来存入mysql的主键信息
    * @param value
    * @param regex
    * @param field
    * @return
    */
  def getFieldFromConcatString(value: String, regex: String, field: String): String = {


//    value.split(regex)(0).substring(field.length + 1)
    val splits = value.split("\\|")
    val list = new util.ArrayList[Any]()
    for (split <- splits)  {
      val index = split.indexOf("=")
      if (split.substring(0, index) == field) {
        val result = split.substring(index + 1)
        list.add(result)
      }
    }

    list.get(0).toString
  }
}
