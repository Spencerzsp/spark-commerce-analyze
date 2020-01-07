package com.wbbigdata

import java.util.{Date, UUID}

import conf.ConfigurationManager
import constant.Constants
import model.{UserInfo, UserVisitAction}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import utils.{DateUtils, ParamUtils, StringUtils, ValidUtils}

object SessionStatistic {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("SessionStatistic").setMaster("local[*]")
    val sparkSession = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAM)
//    println(jsonStr)
    val taskParam = JSONObject.fromObject(jsonStr)

    //存入mysql的主键
    val taskUUID = UUID.randomUUID().toString

    //actionRDD:rdd[UserVisitAction]
    val actionRDD = getActionRDD(sparkSession, taskParam)
//    actionRDD.foreach(println)

    //转换为以sessionid为key，UserVisitAction为value
    //sessionId2Action:rdd[(sid, UserVisitAction)]
    val sessionId2Action = actionRDD.map{
      item => (item.session_id, item)
    }

    //转换为以sessionid为key，Interable[UserVisitAction]为value
    val sessionId2GroupRDD = sessionId2Action.groupByKey()
    sessionId2GroupRDD.cache()

//    sessionId2GroupRDD.foreach(println)

    //获取聚合数据里的聚合信息
    val sessionId2FullInfoRDD = getFullInfoData(sparkSession, sessionId2GroupRDD)
//    sessionId2FullInfoRDD.foreach(println)

    //注册累加器
    val sessionStatAccumulator = new SessionStatAccumulator
    sparkSession.sparkContext.register(sessionStatAccumulator, "sessionStatAccumulator")

    //过滤数据
    val sessionFilterRDD = getFilterData(sparkSession, taskParam, sessionStatAccumulator, sessionId2FullInfoRDD)
    sessionFilterRDD.foreach(println)

  }

  /**
    * 过滤数据
    * @param taskParam
    * @param sessionId2FullInfoRDD
    */
  def getFilterData(sparkSession: SparkSession,
                    taskParam: JSONObject,
                    sessionStatAccumulator: SessionStatAccumulator,
                    sessionId2FullInfoRDD: RDD[(String, String)]) = {

    //从taskParam提取限制条件
    val  startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val searchKeywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val clickCategoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    //判断限制条件是否为空，拼接成过滤信息fullInfo
    var filterInfo = (if(startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if(endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if(professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if(cities != null) Constants.PARAM_CITIES + "=" +cities + "|" else "") +
      (if(sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if(searchKeywords != null) Constants.PARAM_KEYWORDS + "=" + searchKeywords + "|" else "") +
      (if(clickCategoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + clickCategoryIds else "")

    //如果最后不为空，去掉"|"
    if(filterInfo.endsWith("\\|"))
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)

    val sessionId2FilterRDD = sessionId2FullInfoRDD.filter {
      case (sessionid, fullInfo) =>
        var success = true
        if (!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
          success = false
        }
        if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)) {
          success = false
        }
        if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)) {
          success = false
        }
        if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)) {
          success = false
        }
        if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS)) {
          success = false
        }
        if (!ValidUtils.in(fullInfo, Constants.FIELD_CLICK_CATEGORY_IDS, filterInfo, Constants.PARAM_CATEGORY_IDS)) {
          success = false
        }

        if (success) {
          sessionStatAccumulator.add(Constants.SESSION_COUNT)

          val stepLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
          val visitLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong

//          calculateStepLength(stepLength)
//          calculateVisitLength(visitLength)
          calculateStepLength(stepLength, sessionStatAccumulator)
          calculateVisitLength(visitLength, sessionStatAccumulator)
        }

        success
    }
    sessionId2FilterRDD
  }

  def calculateStepLength(stepLength: Long, sessionStatAccumulator: SessionStatAccumulator) = {
    if(stepLength >= 1 && stepLength <= 3) {
      sessionStatAccumulator.add(Constants.STEP_PERIOD_1_3)
    }else if(stepLength >= 4 && stepLength <= 6) {
      sessionStatAccumulator.add(Constants.STEP_PERIOD_4_6)
    }else if(stepLength >= 7 && stepLength <= 9) {
      sessionStatAccumulator.add(Constants.STEP_PERIOD_7_9)
    }else if(stepLength >= 10 && stepLength <= 30) {
      sessionStatAccumulator.add(Constants.STEP_PERIOD_10_30)
    }else if(stepLength > 30 && stepLength <= 60) {
      sessionStatAccumulator.add(Constants.STEP_PERIOD_30_60)
    }else if(stepLength > 60) {
      sessionStatAccumulator.add(Constants.STEP_PERIOD_60)
    }
  }

  def calculateVisitLength(visitLength: Long, sessionStatAccumulator: SessionStatAccumulator) = {
    if(visitLength >= 1 && visitLength <= 3) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    }else if(visitLength >= 4 && visitLength <= 6) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    }else if(visitLength >= 7 && visitLength <= 9) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    }else if(visitLength >= 10 && visitLength <= 30) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    }else if(visitLength > 30 && visitLength <= 60) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_30s_60s)
    }else if(visitLength > 60 && visitLength <= 180) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    }else if(visitLength > 180 && visitLength <= 600) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    }else if(visitLength > 600 && visitLength <= 1800) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    }else if(visitLength > 1800) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_30m);
    }
  }


  /**
    * 获取聚合信息
    * @param sparkSession
    * @param sessionId2GroupRDD
    * @return
    */
  def getFullInfoData(sparkSession: SparkSession, sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])]) = {

    val userId2AggrInfoRDD = sessionId2GroupRDD.map {
      case (sid, iterableAction) =>
        var startTime: Date = null
        var endTime: Date = null

        var userId = -1L
        val searchKeywords = new StringBuffer("")
        val clickCategories = new StringBuffer("")

        var stepLength = 0

        for (action <- iterableAction) {
          if (userId == -1L)
            userId = action.user_id

          val actionTime = DateUtils.parseTime(action.action_time)

          if (startTime == null || startTime.after(actionTime))
            startTime = actionTime

          if (endTime == null || endTime.before(actionTime))
            endTime = actionTime

          val searchkeyWord = action.search_keyword
          val clickCategory = action.click_category_id

          if (StringUtils.isNotEmpty(searchkeyWord) && !searchKeywords.toString.contains(searchkeyWord)) {
            searchKeywords.append(searchkeyWord + ",")
          }
          if (clickCategory != -1L && !clickCategories.toString.contains(clickCategory)) {
            clickCategories.append(clickCategory + ",")
          }
          stepLength += 1
        }

//        println(searchKeywords.toString)


        val searchKw = StringUtils.trimComma(searchKeywords.toString)
        val clickCg = StringUtils.trimComma(clickCategories.toString)

        val visitLength = (endTime.getTime - startTime.getTime) / 1000

        val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sid + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKw + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCg + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)

        (userId, aggrInfo)
    }
//    userId2AggrInfoRDD

    import sparkSession.implicits._

    val sql = "select * from user_info"

    //dataFrame: DataFrame里面是DataSet[Row]
    val dataFrame = sparkSession.sql(sql)

    //dataSet: DataSet[UserInfo]
    val dataSet = dataFrame.as[UserInfo]
    // RDD[UserInfo]
    val userInfoRDD = dataSet.rdd.map{
      user => (user.user_id, user)
    }

    userId2AggrInfoRDD.join(userInfoRDD).map{
      case (userId, (aggrInfo, userInfo)) =>
        val age = userInfo.age
        val professional = userInfo.professional
        val sex = userInfo.sex
        val city = userInfo.city

        //将userInfo表中的信息拼接到需求中
        val fullInfo = aggrInfo + "|" + Constants.FIELD_AGE + "=" +age + "|" +
        Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
        Constants.FIELD_SEX + "=" + sex + "|" +
        Constants.FIELD_CITY + "=" + city

        val sessionId = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)

        (sessionId, fullInfo)
    }

  }

  /**
    * 获取用户行为信息的RDD
    * @param sparkSession
    * @param taskParam
    * @return
    */
  def getActionRDD(sparkSession: SparkSession, taskParam: JSONObject) = {

    //获取配置文件中startDate和endDate的限定条件
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
//    println(startDate)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
//    println(endDate)

    import sparkSession.implicits._

//    val sql = s"select * from user_visit_action where date >= '$startDate' and date <= '$endDate'"
    val sql =
      s"""
        |select * from user_visit_action where date >= '$startDate' and date <= '$endDate'
      """.stripMargin
    //获取到DataFrame,里面是DataSet[Row]
    val dataFrame = sparkSession.sql(sql)

    //DataSet[UserVisitAction],然后再转换为rdd,rdd[UserVisitAction]
    dataFrame.as[UserVisitAction].rdd
  }


}
