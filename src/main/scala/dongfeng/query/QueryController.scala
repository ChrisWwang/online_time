package dongfeng.query

import java.util.Properties

import com.mongodb.spark.MongoSpark
import dongfeng.code.tools.spark.{GlobalConfigUtils, SparkEngine}
import dongfeng.query.sql.VehicleSQL
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode}

object QueryController {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val sparkConf = SparkEngine.sparkConf()

    val sparkSession = SparkEngine.session(sparkConf)

    import sparkSession.sqlContext.implicits._

//    var alliance_business = sparkSession
//      .read
//      .format(GlobalConfigUtils.customHbasePath)
//      .options(
//        Map(
//          GlobalConfigUtils.sparksql_table_schema -> GlobalConfigUtils.alliance_business_SparkSQLSchema,
//          GlobalConfigUtils.hbase_table_name -> GlobalConfigUtils.table_alliance_business,
//          GlobalConfigUtils.hbase_table_schema -> GlobalConfigUtils.alliance_business_HbaseSchema
//        )).load()
//    alliance_business.createOrReplaceTempView("alliance_business")
//
//    alliance_business.show()


    val df: DataFrame = MongoSpark.load(sparkSession)
    df.createOrReplaceTempView("driver_online_record")

    df.show()


    val driver_info: DataFrame = sparkSession.read.format("jdbc")
      .option("url", "jdbc:mysql://47.111.68.2:3306/och_test?useUnicode=true&characterEncoding=utf8")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "driver_info")
      .option("driver", "com.mysql.jdbc.Driver")
      .load()
    //创建车辆信息表
    driver_info.createOrReplaceTempView("driver_info")


    // 外联车辆信息表
    val order_info: DataFrame = sparkSession.read.format("jdbc")
      .option("url", "jdbc:mysql://47.111.68.2:3306/och_test?useUnicode=true&characterEncoding=utf8")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "order_info_201907")
      .option("driver", "com.mysql.jdbc.Driver")
      .load()
    //创建车辆信息表
    order_info.createOrReplaceTempView("order_info")


    val opt_alliance_business: DataFrame = sparkSession.read.format("jdbc")
      .option("url", "jdbc:mysql://47.111.68.2:3306/och_test?useUnicode=true&characterEncoding=utf8")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "opt_alliance_business")
      .option("driver", "com.mysql.jdbc.Driver")
      .load()
    //创建车辆信息表
    opt_alliance_business.createOrReplaceTempView("opt_alliance_business")


    //######################统计司机活动时间###################
    sparkSession.sql(VehicleSQL.driver_online_record_time_rank).createOrReplaceTempView("driver_online_record_time_rank")
    sparkSession.sql(VehicleSQL.driver_online_record_time_rank_desc).createOrReplaceTempView("driver_online_record_time_rank_desc")
    sparkSession.sql(VehicleSQL.driver_online_record_sameday).createOrReplaceTempView("driver_online_record_sameday")
    sparkSession.sql(VehicleSQL.driver_online_record_yesterday).createOrReplaceTempView("driver_online_record_yesterday")
    sparkSession.sql(VehicleSQL.driver_online_record_nextday).createOrReplaceTempView("driver_online_record_nextday")
    sparkSession.sql(VehicleSQL.order_info_sameday).createOrReplaceTempView("order_info_sameday")
    sparkSession.sql(VehicleSQL.order_info_yesterday).createOrReplaceTempView("order_info_yesterday")
    sparkSession.sql(VehicleSQL.order_info_nextday).createOrReplaceTempView("order_info_nextday")
    sparkSession.sql(VehicleSQL.order_info_oneday).createOrReplaceTempView("order_info_oneday")
    sparkSession.sql(VehicleSQL.driver_info_join_opt_alliance_business).createOrReplaceTempView("driver_info_join_opt_alliance_business")
    sparkSession.sql(VehicleSQL.order_info_join_driver_info).createOrReplaceTempView("order_info_join_driver_info")
    val frame: DataFrame = sparkSession.sql(VehicleSQL.order_info_oneday_group1)
    frame.show()



    // 测试数据库
    //    val url = "jdbc:mysql://120.27.208.185:3306/och_test?useUnicode=true&characterEncoding=utf8"
    //    val connectionProperties = new Properties()
    //    connectionProperties.setProperty("user", "och_test"); // 设置用户名
    //    connectionProperties.setProperty("password", "Kop21IQ8"); // 设置密码
    //    df1.write.mode(SaveMode.Append).jdbc(url, "driver_online_time", connectionProperties)


//    val url = "jdbc:mysql://47.111.68.2:3306/och_test?useUnicode=true&characterEncoding=utf8"
//    val connectionProperties = new Properties()
//    connectionProperties.setProperty("user", "root"); // 设置用户名
//    connectionProperties.setProperty("password", "123456"); // 设置密码
//    frame.write.mode(SaveMode.Append).jdbc(url, "driver_online_time_copy", connectionProperties)


    sparkSession.stop()
    System.exit(0)

  }
}
