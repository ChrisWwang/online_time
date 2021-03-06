package dongfeng.query.process

import dongfeng.query.sql.OtherDayVehicleSQL3
import org.apache.spark.sql.{DataFrame, SparkSession}

object MongoDBFilter_4000_6000 {

  def order_info_filter(sparkSession : SparkSession,driver_online_record_time_original : DataFrame): DataFrame = {

    driver_online_record_time_original.createOrReplaceTempView("driver_online_record_time_original")


    val driver_online_record_time_original_3000_6000 = sparkSession.sql(OtherDayVehicleSQL3.driver_online_record_time_original_3000_6000)

    driver_online_record_time_original_3000_6000
  }

}
