package dongfeng.query.begin

import dongfeng.query.process.{MySqlFilter, MySqlOperation}
import org.apache.spark.sql.{DataFrame, SparkSession}

object MySqlBegin {

  def mysql_begin_onlinetime(sparkSession : SparkSession): DataFrame = {

    val order_info_filter = MySqlFilter.order_info_filter(sparkSession)
    order_info_filter.cache()


    val driver_online_mysql_oneday: DataFrame = MySqlOperation.order_info_mysql_operation(sparkSession,order_info_filter)

    driver_online_mysql_oneday
  }

}
