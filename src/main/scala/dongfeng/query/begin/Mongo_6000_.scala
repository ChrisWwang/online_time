package dongfeng.query.begin

import dongfeng.query.process.{MongoDBFilter_6000_, MongoDBOperation, MongoDBOperation2}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Mongo_6000_ {

  def mongo_6000_onlinetime(sparkSession : SparkSession,driver_online_record_time_original : DataFrame): DataFrame = {

    val driver_online_record_time_original_6000_ = MongoDBFilter_6000_.order_info_filter(sparkSession,driver_online_record_time_original)
    driver_online_record_time_original_6000_.cache()
    val driver_online_record_time_rank: DataFrame = MongoDBOperation.order_info_filter_0(sparkSession, driver_online_record_time_original_6000_)
    val driver_online_mongo_oneday: DataFrame = MongoDBOperation2.order_info_mongo_operation(sparkSession, driver_online_record_time_rank)

    driver_online_mongo_oneday
  }

}
