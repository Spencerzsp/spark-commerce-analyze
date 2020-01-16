package com.wbbigdata

import java.util.{Date, UUID}

import conf.ConfigurationManager
import constant.Constants
import model.{SessionRandomExtract, Top10Category, UserInfo, UserVisitAction}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import utils.{DateUtils, ParamUtils, StringUtils, ValidUtils}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

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
    val sessionId2ActionRDD = actionRDD.map{
      item => (item.session_id, item)
    }

    //转换为以sessionid为key，Interable[UserVisitAction]为value
    val sessionId2GroupRDD = sessionId2ActionRDD.groupByKey()
    sessionId2GroupRDD.cache()

//    sessionId2GroupRDD.foreach(println)

    //获取聚合数据里的聚合信息
    val sessionId2FullInfoRDD = getFullInfoData(sparkSession, sessionId2GroupRDD)
//    sessionId2FullInfoRDD.foreach(println)

    //注册累加器
    val sessionStatAccumulator = new SessionStatAccumulator
    sparkSession.sparkContext.register(sessionStatAccumulator, "sessionStatAccumulator")

    //需求一：获取符合过滤条件的数据
    //过滤数据
    //sessionId2FilterRDD: RDD[sid, fullInfo]
    val sessionId2FilterRDD = getFilterRDD(sparkSession, taskParam, sessionStatAccumulator, sessionId2FullInfoRDD)
//    sessionId2FilterRDD.foreach(println)
    //统计符合限制条件的数据的条数
//    println(sessionId2FilterRDD.count())

    //需求二：session随机抽取
//    sessionRandomExtract(sparkSession,taskUUID, sessionId2FilterRDD)

    //获取所有符合过滤条件的action数据
    val sessionId2FilterActionRDD = sessionId2ActionRDD.join(sessionId2FilterRDD).map {
      case (sessionId, (action, fullInfo)) =>
        (sessionId, action)
    }

    //获取点击品类在前十的商品，并将数据存进mysql
    top10PopularCategories(sparkSession, taskUUID, sessionId2FilterActionRDD)


  }

  /**
    * 获取点击次数
    * @param sessionId2FilterActionRDD
    * @return
    */
  def getClickCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {

//    val clickFilterRDD = sessionId2FilterActionRDD.filter{
//      case (sessionid, action) => action.click_category_id != -1L
//    }

    //先进行过滤，把点击行为对应的action过滤出来
    val clickFilterRDD = sessionId2FilterActionRDD.filter(_._2.click_category_id != -1L)
    //进行格式转换，为reduceBykey做准备
    val clickNumRDD = clickFilterRDD.map{
      case (sessionId, action) => (action.click_category_id, 1L)
    }

    clickNumRDD.reduceByKey(_ + _)

  }


  /**
    * 获取下单次数
    * @param sessionId2FilterActionRDD
    * @return
    */
  def getOrderCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {

    val orderFilterRDD = sessionId2FilterActionRDD.filter(_._2.order_category_ids != null)
    //order_category_ids包含做个，因此使用flatMap扁平化，再对扁平化后的每一个元素进行格式转换
    val orderNumRDD = orderFilterRDD.flatMap {
      case (sessionid, action) =>
        action.order_category_ids.split(",").map(item => (item.toLong, 1L))
    }
    orderNumRDD.reduceByKey(_ + _)
  }

  /**
    * 获取付款次数
    * @param sessionId2FilterActionRDD
    * @return
    */
  def getPayCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {

    val payFilterRDD = sessionId2FilterActionRDD.filter(_._2.pay_category_ids != null)

    val payNumRDD = payFilterRDD.flatMap {
      case (sessionId, action) =>
        action.pay_category_ids.split(",").map(item => (item.toLong, 1L))
    }
    payNumRDD.reduceByKey(_ + _)
  }

  /**
    * 获取品类的点击次数、下单次数、付款次数
    * @param cid2CidRDD
    * @param cid2ClickCountRDD
    * @param cid2OrderCountRDD
    * @param cid2PayCountRDD
    * @return
    */
  def getFullCount(cid2CidRDD: RDD[(Long, Long)],
                   cid2ClickCountRDD: RDD[(Long, Long)],
                   cid2OrderCountRDD: RDD[(Long, Long)],
                   cid2PayCountRDD: RDD[(Long, Long)]) = {
    val cid2ClickInfoRDD = cid2CidRDD.leftOuterJoin(cid2ClickCountRDD).map{
      case (cid, (categoryId, option)) =>
        val clickCount = if (option.isDefined) option.get else 0
        val aggrInfo = Constants.FIELD_CATEGORY_ID + "=" + cid + "|" +
        Constants.FIELD_CLICK_COUNT + "=" + clickCount

        (cid, aggrInfo)
    }

    val cid2OrderInfoRDD = cid2ClickInfoRDD.leftOuterJoin(cid2OrderCountRDD).map{
      case (cid, (clickInfo, option)) =>
        val orderCount = if (option.isDefined) option.get else 0
        val aggrInfo = clickInfo + "|" +
        Constants.FIELD_ORDER_COUNT + "=" + orderCount

        (cid, aggrInfo)
    }

    val cid2PayInfoRDD = cid2OrderInfoRDD.leftOuterJoin(cid2PayCountRDD).map {
      case (cid, (orderInfo, option)) =>
        val payCount = if (option.isDefined) option.get else 0
        val aggrInfo = orderInfo + "|" +
          Constants.FIELD_PAY_COUNT + "=" + payCount

        (cid, aggrInfo)
    }

    cid2PayInfoRDD


  }

  /**
    * 获取前十热门受欢迎种类商品
    * 二次排序SortKey的实现
    * @param sparkSession
    * @param taskUUID
    * @param sessionId2FilterActionRDD
    */
  def top10PopularCategories(sparkSession: SparkSession,
                             taskUUID: String,
                             sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]): Unit = {
    //获取所有发生过点击、下单、付款的品类
    var cid2CidRDD = sessionId2FilterActionRDD.flatMap{
      case (sid, action) =>
        val categoryBuffer = new ArrayBuffer[(Long, Long)]()

        //点击行为
        if(action.click_category_id != -1){
          categoryBuffer += ((action.click_category_id, action.click_category_id))
        }else if (action.order_category_ids != null) {
          for (orderCid <- action.order_category_ids.split(","))
            categoryBuffer += ((orderCid.toLong, orderCid.toLong))
        }else if(action.pay_category_ids != null){
          for(payCid <- action.pay_category_ids)
            categoryBuffer += ((payCid.toLong, payCid.toLong))
        }

        categoryBuffer
    }

    cid2CidRDD = cid2CidRDD.distinct()

    //第二步：统计品类点击的次数、统计下单的次数、统计付款的次数
    val cid2ClickCountRDD = getClickCount(sessionId2FilterActionRDD)
    val cid2OrderCountRDD = getOrderCount(sessionId2FilterActionRDD)
    val cid2PayCountRDD = getPayCount(sessionId2FilterActionRDD)

    //(19,categoryid=19|clickCount=543|orderCount=0|payCount=598)
    //cid2FullCountRDD: RDD[(cid, countInfo)]
    val cid2FullCountRDD = getFullCount(cid2CidRDD, cid2ClickCountRDD, cid2OrderCountRDD, cid2PayCountRDD)
//    cid2FullCountRDD.foreach(println)

    //实现自定义二次排序key,默认是自然排序(升序)
    val sortKey2FullCountRDD = cid2FullCountRDD.map{
      case (cid, countInfo) =>
        val clickCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
        val orderCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
        val payCount = StringUtils.getFieldFromConcatString(countInfo, "\\|" , Constants.FIELD_PAY_COUNT).toLong

        val sorkKey = SorkKey(clickCount, orderCount, payCount)

        (sorkKey, countInfo)
    }

    //传递false，表示降序
    val top10CategoryArray = sortKey2FullCountRDD.sortByKey(false).take(10)
    top10CategoryArray.foreach(println)

    //构造RDD
    val top10CategoryRDD = sparkSession.sparkContext.makeRDD(top10CategoryArray).map{
      case (sortKey,countInfo) =>
        //获取对应字段信息，为封装到case Top10Category做准备
        val cid = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        val clickCount = sortKey.clickCount
        val orderCount = sortKey.orderCount
        val payCount = sortKey.payCount

        //将封装好的数据返回
        Top10Category(taskUUID, cid, clickCount, orderCount, payCount)
    }

    import sparkSession.implicits._
    top10CategoryRDD.toDF.show(truncate = false)

    top10CategoryRDD.toDF.write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_category")
      .mode(SaveMode.Overwrite)
      .save()

  }

  /**
    * session随机抽取，并存入mysql
    * @param sparkSession
    * @param taskUUID
    * @param sessionId2FilterRDD
    */
  def sessionRandomExtract(sparkSession: SparkSession,
                           taskUUID: String,
                           sessionId2FilterRDD: RDD[(String, String)]): Unit = {

    //dateHour2FullInfoRDD: RDD[(dateHour, fullInfo)]
    val dateHour2FullInfoRDD = sessionId2FilterRDD.map {
      case (sid, fullInfo) =>
        val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
        val dateHour = DateUtils.getDateHour(startTime)

        (dateHour, fullInfo)
    }

//    dateHour2FullInfoRDD.foreach(println)

    //获取每个小时session的数量
    //hourCountMap: Map[(dateHour, count)]
    val hourCountMap = dateHour2FullInfoRDD.countByKey()

//    for ((dateHour, count) <- hourCountMap) {
//      println(dateHour, count)
//    }
    //获取每天的session数量
    //dateHourCountMap: Map[(date, Map[(hour, count)])]
    val dateHourCountMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]()

    //遍历dateHourCountMap
    for ((dateHour, count) <- hourCountMap) {
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)

      dateHourCountMap.get(date) match {
        case None => dateHourCountMap(date) = new mutable.HashMap[String, Long]()
          dateHourCountMap(date) += (hour->count)
        case Some(map) => dateHourCountMap(date) += (hour->count)
      }

      //解决问题一：一共又多少天:dateHourCountMap.size
      //           一天抽取多少条：100/dateHourCountMap.size
      val extractPerDay = 100 / dateHourCountMap.size

      //解决问题二：一天有多少session：dateHourCountMap(date).value.sum
      //解决问题三：一个小时多少session：dateHourCountMap(date)(hour)

      //生成每天随机抽取session的下表
      val dateHourExtractIndexListMap = new mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]()

      //遍历之前填充完的dateHourCountMap
      for ((date, hourCountMap) <- dateHourCountMap) {

        val dateSessionCount = hourCountMap.values.sum
        dateHourExtractIndexListMap.get(date) match {
          case None => dateHourExtractIndexListMap(date) = new mutable.HashMap[String, ListBuffer[Int]]()
            generateRandomIndexList(extractPerDay, dateSessionCount,hourCountMap, dateHourExtractIndexListMap(date))
          case Some(map) =>
            generateRandomIndexList(extractPerDay, dateSessionCount,hourCountMap, dateHourExtractIndexListMap(date))

        }

//        dateHourExtractIndexListMap.foreach(println)

        //到目前为止，我们获取到了每个小时要抽取的session的index
        //使用广播变量进行广播,广播大变量，提升任务性能
        val dateHourExtractIndexListMapBd = sparkSession.sparkContext.broadcast(dateHourExtractIndexListMap)

        //dateHour2FullInfoRDD: RDD[(dateHour, fullInfo)]
        //dateHour2GroupRDD: rdd[(dateHour, iterableFullInfo)]
        val dateHour2GroupRDD = dateHour2FullInfoRDD.groupByKey()

//        dateHour2GroupRDD.foreach(println)

        //extractSessionRDD: RDD[SessionRandomExtract]
        val extractSessionRDD = dateHour2GroupRDD.flatMap{
          case (dateHour, iterableFullInfo) =>
            val date = dateHour.split("_")(0)
            val hour = dateHour.split("_")(1)

            val extractList = dateHourExtractIndexListMapBd.value.get(date).get(hour)
            extractList.foreach(println)

            //创建一个容器，当有符合的数据就写入容器中
            val extractSessionArrayBuffer = new ArrayBuffer[SessionRandomExtract]()
            var index = 0
            for (fullInfo <- iterableFullInfo) {
              if (extractList.contains(index)){
                val sessionId = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SESSION_ID)
                val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
                val searchKeywords = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
                val clickCategories = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)

                val extractSession = SessionRandomExtract(taskUUID, sessionId, startTime, searchKeywords, clickCategories)
                extractSessionArrayBuffer += extractSession
              }
              index += 1
            }
          extractSessionArrayBuffer
        }

        import sparkSession.implicits._
        extractSessionRDD.toDF().write
          .format("jdbc")
          .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
          .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
          .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
          .option("dbtable", "session_extract")
          .mode(SaveMode.Append)
          .save()

      }


    }

  }

  /**
    * 生成抽取session的随机下标列表
    * @param extractPerDay
    * @param daySessionCount
    * @param hourCountMap
    * @param hourListMap
    */
  def generateRandomIndexList(extractPerDay: Long,
                              daySessionCount: Long,
                              hourCountMap: mutable.HashMap[String, Long],
                              hourListMap: mutable.HashMap[String, ListBuffer[Int]]): Unit ={

    for ((hour, count) <- hourCountMap) {
      //获取一个小时要抽取多少数据
      var hourExtractCount = ((count / daySessionCount.toDouble) * extractPerDay).toInt
      //避免一个小时要抽取的数据超过这个小时的总数
      if (hourExtractCount > count)
        hourExtractCount = count.toInt

      val random = new Random()
      hourListMap.get(hour) match {
        case None => hourListMap(hour) = new ListBuffer[Int]
          for (i <- 0 until hourExtractCount){
            var index = random.nextInt(count.toInt)
            while (hourListMap(hour).contains(index)){
              index = random.nextInt(count.toInt)
            }
            hourListMap(hour).append(index)
          }
        case Some(list) =>
          for (i <- 0 until hourExtractCount){
          var index = random.nextInt(count.toInt)
          while (hourListMap(hour).contains(index)){
            index = random.nextInt(count.toInt)
          }
          hourListMap(hour).append(index)
        }
      }

    }
  }


  /**
    * 过滤数据RDD[(sessionId, filterInfo)]
    * @param taskParam
    * @param sessionId2FullInfoRDD
    */
  def getFilterRDD(sparkSession: SparkSession,
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

      //startAge=20|endAge=50|professionals=|cities=|sex=|keywords=|categoryIds=
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)

    val sessionId2FilterRDD = sessionId2FullInfoRDD.filter {
      case (sessionid, fullInfo) =>
        var success = true
        //(sessionid=63746095035e4c89a5b5bd0bd82e9988|searchwords=华为手机,小龙虾,保温杯,卫生纸,苹果耳机,RedmiBook,MacBook,小米笔记本,吸尘器|clickCategoryIds=49,67,31,42,19,95,22,27,86,24,9,12,69,30,20,30,96|visitLength=3487|stepLength=84|startTime=2020-01-13 02:00:06|age=18|professional=professional87|sex=female|city=city94)
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
        if (!ValidUtils.in(fullInfo, Constants.FIELD_CATEGORY_ID, filterInfo, Constants.PARAM_CATEGORY_IDS)) {
          success = false
        }

        if (success) {
          sessionStatAccumulator.add(Constants.SESSION_COUNT)

          def calculateStepLength(stepLength: Long) = {
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

          def calculateVisitLength(visitLength: Long) = {
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


          val stepLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
          val visitLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong

          calculateStepLength(stepLength)
          calculateVisitLength(visitLength)

        }

        success
    }
    sessionId2FilterRDD
  }

  /**
    * 获取聚合信息RDD[(sessionId, fullInfo)]
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
    * 获取用户行为信息的RDD:RDD[UserVisitAction]
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

    //DataSet[UserVisitAction],然后再转换为RDD[UserVisitAction]
    dataFrame.as[UserVisitAction].rdd
  }


}
