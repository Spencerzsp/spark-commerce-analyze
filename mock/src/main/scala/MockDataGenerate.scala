import java.util.UUID

import model.{ProductInfo, UserInfo, UserVisitAction}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.DataUtis

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object MockDataGenerate {

  /**
    * 模拟产品数据表
    * @return
    */
  def mockProductInfo() = {

    val rows = ArrayBuffer[ProductInfo]()
    val random = new Random()
    val productStatus = Array(0, 1)

    // 随机产生100个产品信息
    for(i <- 0 to 100) {
      val productId = i
      val productName = "product" + i
      val extendInfo = "{\"product_status\": " + productStatus(random.nextInt(2)) + "}"

      rows += ProductInfo(productId, productName,extendInfo)
    }

    rows.toArray
  }


  /**
    * 模拟用户信息表
    *
    * @return
    */
  def mockUserInfo() = {

    val rows = ArrayBuffer[UserInfo]()
    val sexes = Array("male", "female")
    val random = new Random()

    // 随机产生100个用户的i信息
    for (i <- 0 to 100) {
      val userid = i
      val username = "user" + i
      val name = "name" + i
      val age = random.nextInt(60)
      val professional = "professional" + i
      val city = "city" + random.nextInt(100)
      val sex = sexes(random.nextInt(2))

      rows += UserInfo(userid, username, name, age, professional, city, sex)
    }

    rows.toArray
  }


  /**
    * 模拟用户行为信息
    *
    * @return
    */
  def mockUserVisitActionData() = {

    val searchKeywords = Array("华为手机", "联想笔记本", "小龙虾", "卫生纸", "吸尘器", "保温杯", "MacBook", "小米笔记本", "RedmiBook", "苹果耳机")

    //yyyy-MM-dd
    val date = DataUtis.getTodayDate()

    //关注四个行为: 搜索、点击、下单、付款
    val actions = Array("search", "click", "order", "pay")
    val random = new Random()
    val rows = ArrayBuffer[UserVisitAction]()

    //一共100个用户，有重复
    for(i <- 1 to 100) {
      val userid = random.nextInt(100)

      //每个用户产生10个session
      for(j <- 1 to 10) {

        //不可变的，全局的，独一无二的128bit长度的标识符，用于标识一个session
        val sessionid = UUID.randomUUID().toString.replace("-", "")

        // 在yyyy-MM-dd后面添加一个随机的小时时间(0-23)
        val baseActionTime = date + " " + random.nextInt(23)

        //每个(userid+sessionid)生成0-100条用户访问数据
        for (k <- 0 to random.nextInt(100)) {

          val pageid = random.nextInt(10)

          // 在yyyy-MM-dd HH后面添加一个随机的分钟时间和秒时间
          val actionTime = baseActionTime + ":" + String.valueOf(random.nextInt(59)) + ":"+ String.valueOf(random.nextInt(59))

          var searchKeyword: String = null
          var clickCategoryId: Long = -1L
          var clickProductId: Long = -1L
          var orderCategoryIds: String = null
          var orderProductIds: String = null
          var payCategoryIds: String = null
          var payProductIds: String = null
          val cityid = random.nextInt(10).toLong

          // 随机确定用户在当前session中的行为
          val action = actions(random.nextInt(4))

          // 根据随机产生的用户行为action决定对应字段的值
          action match {
            case "search" => searchKeyword = searchKeywords(random.nextInt(10))
            case "click" => clickCategoryId = random.nextInt(100).toLong
              clickProductId = String.valueOf(random.nextInt(100)).toLong
            case "order" => orderProductIds = random.nextInt(100).toString
              orderProductIds = random.nextInt(100).toString
            case "pay" => payCategoryIds = random.nextInt(100).toString
              payProductIds = random.nextInt(100).toString

          }

          rows += UserVisitAction(date, userid, sessionid,
            pageid, actionTime, searchKeyword,
            clickCategoryId, clickProductId,
            orderCategoryIds, orderProductIds,
            payCategoryIds, payProductIds, cityid)
        }
      }
    }

    rows.toArray
  }

  /**
    *
    * @param spark
    * @param tableName
    * @param dataDF
    * @return
    */
  def insertHive(spark: SparkSession, tableName: String, dataDF: DataFrame) = {
//    spark.sql("DROP TABLE IF EXISTS " + tableName)
    dataDF.write.mode("append").saveAsTable(tableName)
  }

  val USER_VISIT_ACTION_TABLE = "user_visit_action"
  val USER_INFO_TABLE = "user_info"
  val PRODUCT_INFO_TABLE = "product_info"

  /**
    * 主入口方法
    * @param args
    */
  def main(args: Array[String]): Unit = {

    //创建spark配置
    val conf = new SparkConf()
      .setAppName("MockDataGenerate")
      .setMaster("local[*]")

    //创建spark sql客户端
    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
    
    //模拟数据
    val userVisitActionData = this.mockUserVisitActionData()
    val userInfoData = this.mockUserInfo()
    val productInfoData = this.mockProductInfo()

    // 模拟数据转换成RDD
    val userVisitActionRdd = spark.sparkContext.makeRDD(userVisitActionData)
    val userInfoRdd = spark.sparkContext.makeRDD(userInfoData)
    val productInfoRdd = spark.sparkContext.makeRDD(productInfoData)

    // 加载spark sql的隐式转换
    import spark.implicits._

    // 将用户访问数据装欢为DF保存到hive表中
    val userVisitActionDF = userVisitActionRdd.toDF()
    insertHive(spark, USER_VISIT_ACTION_TABLE, userVisitActionDF)

    // 将用户信息数据转换为DF保存到hive表中
    val userInfoDF = userInfoRdd.toDF()
    insertHive(spark, USER_INFO_TABLE, userInfoDF)

    // 将产品信息转换为DF保存到hive表中
    val productInfoDF = productInfoRdd.toDF()
    insertHive(spark, PRODUCT_INFO_TABLE, productInfoDF)

    spark.close()
  }

}
