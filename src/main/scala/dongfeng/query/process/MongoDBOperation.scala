package dongfeng.query.process

import dongfeng.query.sql.OtherDayVehicleSQL3
import org.apache.spark.sql.{DataFrame, SparkSession}

object MongoDBOperation {

  def order_info_filter_0(sparkSession:SparkSession,driver_online_record_time_original : DataFrame): DataFrame = {

    driver_online_record_time_original.createOrReplaceTempView("driver_online_record_time_original")

    sparkSession.sql(OtherDayVehicleSQL3.driver_online_record_time_state_0).createOrReplaceTempView("driver_online_record_time_state_0")
    sparkSession.sql(OtherDayVehicleSQL3.driver_online_record_time_state_1).createOrReplaceTempView("driver_online_record_time_state_1")
    sparkSession.sql(OtherDayVehicleSQL3.driver_online_record_time_all).createOrReplaceTempView("driver_online_record_time_all")
    sparkSession.sql(OtherDayVehicleSQL3.driver_online_record_time_filter).createOrReplaceTempView("driver_online_record_time_filter")
    val driver_online_record_time_rank: DataFrame = sparkSession.sql(OtherDayVehicleSQL3.driver_online_record_time_rank)


    driver_online_record_time_rank

  }

}
