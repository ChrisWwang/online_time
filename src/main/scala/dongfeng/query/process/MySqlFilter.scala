package dongfeng.query.process

import dongfeng.query.sql.OtherDayVehicleSQL3
import dongfeng.query.tips.Daynum
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object MySqlFilter {

  val month: Int = Daynum.getMonth()

  def order_info_filter(sparkSession: SparkSession): DataFrame = {

    //<<<<<生产环境>>>>>读取mysql数据库的order_info表,每月修改一次
    val order_info: DataFrame = sparkSession.read.format("jdbc")
      .option("url", "jdbc:mysql://10.0.15.145:3306/och_prd?useUnicode=true&characterEncoding=utf8")
      .option("user", "och_prd")
      .option("password", "59whJDFQ")
      .option("dbtable", s"order_info_$month")
      .option("driver", "com.mysql.jdbc.Driver")
      .load()
    order_info.createOrReplaceTempView("order_info")


    val order_info_filter: DataFrame = sparkSession.sql(OtherDayVehicleSQL3.order_info_filter)

    order_info_filter

  }

}
