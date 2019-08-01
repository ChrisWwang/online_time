package dongfeng.query.process

import dongfeng.query.sql.OtherDayVehicleSQL3
import org.apache.spark.sql.{DataFrame, SparkSession}

object MongoDBOperation2 {

  def order_info_mongo_operation(sparkSession:SparkSession,driver_online_record_time_rank : DataFrame): DataFrame = {

    driver_online_record_time_rank.createOrReplaceTempView("driver_online_record_time_rank")

    sparkSession.sql(OtherDayVehicleSQL3.driver_online_record_sameday).createOrReplaceTempView("driver_online_record_sameday")

    sparkSession.sql(OtherDayVehicleSQL3.driver_online_record_yesterday).createOrReplaceTempView("driver_online_record_yesterday")
    sparkSession.sql(OtherDayVehicleSQL3.driver_online_record_time_rank_desc).createOrReplaceTempView("driver_online_record_time_rank_desc")
    sparkSession.sql(OtherDayVehicleSQL3.driver_online_record_nextday).createOrReplaceTempView("driver_online_record_nextday")
//    sparkSession.sql(OtherDayVehicleSQL3.driver_online_record_time_theday).createOrReplaceTempView("driver_online_record_time_theday")
//    sparkSession.sql(OtherDayVehicleSQL3.driver_online_record_sameday).createOrReplaceTempView("driver_online_record_sameday")

    val driver_online_mongo_oneday: DataFrame = sparkSession.sql(OtherDayVehicleSQL3.driver_online_mongo_oneday)

    driver_online_mongo_oneday

  }

}