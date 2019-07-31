package dongfeng.query.begin

import dongfeng.query.process.{MongoDBFilter, MongoDBFilter_0_3000, MongoDBOperation, MongoDBOperation2}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Mongo_0_3000 {

  def mongo_0_3000_onlinetime(sparkSession : SparkSession,driver_online_record_time_original : DataFrame): DataFrame = {

    val driver_online_record_time_original_0_3000: DataFrame = MongoDBFilter_0_3000.order_info_filter(sparkSession,driver_online_record_time_original)
    driver_online_record_time_original_0_3000.cache()
    val driver_online_record_time_rank: DataFrame = MongoDBOperation.order_info_filter_0(sparkSession, driver_online_record_time_original_0_3000)
    val driver_online_mongo_oneday: DataFrame = MongoDBOperation2.order_info_mongo_operation(sparkSession, driver_online_record_time_rank)

    driver_online_mongo_oneday
  }

}
