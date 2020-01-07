package utils

import net.sf.json.JSONObject

object ParamUtils {

  /**
    * 从JSON对象中提取参数
    * @param jsonObject
    * @param field
    * @return
    */
  def getParam(jsonObject: JSONObject, field: String) = {

    jsonObject.getString(field)
  }

}
