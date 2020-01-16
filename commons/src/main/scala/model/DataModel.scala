package model

object DataModel {

}

/**
  *
  * @param date
  * @param user_id
  * @param session_id
  * @param page_id
  * @param action_time
  * @param search_keyword
  * @param click_category_id
  * @param click_product_id
  * @param order_category_ids
  * @param order_product_ids
  * @param pay_category_ids
  * @param pay_product_ids
  * @param city_id
  */
case class UserVisitAction (date: String,
                             user_id: Long,
                             session_id: String,
                             page_id: Long,
                             action_time: String,
                             search_keyword: String,
                             click_category_id: Long,
                             click_product_id: Long,
                             order_category_ids: String,
                             order_product_ids: String,
                             pay_category_ids: String,
                             pay_product_ids: String,
                             city_id: Long
                           )


/**
  *
  * @param user_id
  * @param username
  * @param name
  * @param age
  * @param professional
  * @param city
  * @param sex
  */
case class UserInfo (user_id: Long,
                      username: String,
                      name: String,
                      age: Int,
                      professional: String,
                      city: String,
                      sex: String
                    )

/**
  *
  * @param product_id
  * @param product_name
  * @param extend_info
  */
case class ProductInfo (product_id: Long,
                        product_name: String,
                        extend_info: String
                       )

case class SessionRandomExtract(taskid: String,
                                sessionid: String,
                                startTime: String,
                                searchKeywords: String,
                                clickCategoryIds: String
                               )

case class Top10Category(taskid: String,
                         categoryId: Long,
                         clickCount: Long,
                         orderCount: Long,
                         payCount: Long
                        )