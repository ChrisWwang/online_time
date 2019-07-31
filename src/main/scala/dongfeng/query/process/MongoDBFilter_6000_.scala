package dongfeng.query.process

import dongfeng.query.sql.OtherDayVehicleSQL3
import org.apache.spark.sql.{DataFrame, SparkSession}

object MongoDBFilter_6000_ {

  def order_info_filter(sparkSession : SparkSession,driver_online_record_time_original : DataFrame): DataFrame = {

    driver_online_record_time_original.createOrReplaceTempView("driver_online_record_time_original")


    val driver_online_record_time_original_6000_ = sparkSession.sql(OtherDayVehicleSQL3.driver_online_record_time_original_6000_)


    driver_online_record_time_original_6000_
  }

}
