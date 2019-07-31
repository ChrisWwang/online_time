package dongfeng.query.process

import dongfeng.query.sql.OtherDayVehicleSQL3
import org.apache.spark.sql.{DataFrame, SparkSession}

object MySqlOperation {

  def order_info_mysql_operation(sparkSession:SparkSession,order_info_filter : DataFrame): DataFrame = {

    order_info_filter.createOrReplaceTempView("order_info_filter")

    sparkSession.sql(OtherDayVehicleSQL3.order_info_sameday).createOrReplaceTempView("order_info_sameday")
    sparkSession.sql(OtherDayVehicleSQL3.order_info_yesterday).createOrReplaceTempView("order_info_yesterday")
    sparkSession.sql(OtherDayVehicleSQL3.order_info_nextday).createOrReplaceTempView("order_info_nextday")
    val driver_online_mysql_oneday: DataFrame = sparkSession.sql(OtherDayVehicleSQL3.driver_online_mysql_oneday)


    driver_online_mysql_oneday
  }


}
