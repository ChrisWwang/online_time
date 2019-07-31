package dongfeng.query.process

import com.mongodb.spark.MongoSpark
import dongfeng.query.sql.OtherDayVehicleSQL3
import org.apache.spark.sql.{DataFrame, SparkSession}

object MongoDBFilter {

  def order_info_filter(sparkSession : SparkSession): DataFrame = {


    val driver_online_record: DataFrame = MongoSpark.load(sparkSession).repartition(1)
    driver_online_record.createOrReplaceTempView("driver_online_record")


    val driver_online_record_time_original: DataFrame = sparkSession.sql(OtherDayVehicleSQL3.driver_online_record_time_original)

    driver_online_record_time_original

  }

}
