package dongfeng.query.begin

import dongfeng.query.process.MongoDBFilter
import dongfeng.query.sql.OtherDayVehicleSQL3
import org.apache.spark.sql.{DataFrame, SparkSession}

object MongoBegin {

  def mongo_begin_onlinetime(sparkSession: SparkSession): DataFrame = {

    val driver_online_record_time_original = MongoDBFilter.order_info_filter(sparkSession)
    driver_online_record_time_original.cache()

    val driver_online_mongo_oneday_0_3000 = Mongo_0_3000.mongo_0_3000_onlinetime(sparkSession, driver_online_record_time_original)

    driver_online_mongo_oneday_0_3000.createOrReplaceTempView("driver_online_mongo_oneday_0_3000")

    val driver_online_mongo_oneday_3000_6000 = Mongo_3000_6000.mongo_3000_6000_onlinetime(sparkSession, driver_online_record_time_original)
    driver_online_mongo_oneday_3000_6000.createOrReplaceTempView("driver_online_mongo_oneday_3000_6000")

    val driver_online_mongo_oneday_6000_ = Mongo_6000_.mongo_6000_onlinetime(sparkSession, driver_online_record_time_original)
    driver_online_mongo_oneday_6000_.createOrReplaceTempView("driver_online_mongo_oneday_6000_")

    //    val driver_online_mongo_oneday_4000_5000 = Mongo_4000_5000.mongo_3000_6000_onlinetime(sparkSession,driver_online_record_time_original)
    //    driver_online_mongo_oneday_4000_5000.createOrReplaceTempView("driver_online_mongo_oneday_4000_5000")

    val driver_online_mongo_oneday = sparkSession.sql(OtherDayVehicleSQL3.driver_online_mongo_oneday_all)

    driver_online_mongo_oneday
  }


}
