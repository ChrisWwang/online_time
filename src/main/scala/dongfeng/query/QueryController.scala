package dongfeng.query

import java.util.Properties

import com.mongodb.spark.MongoSpark
import dongfeng.code.tools.spark.{GlobalConfigUtils, SparkEngine}
import dongfeng.query.hbase.readHbase
import dongfeng.query.sql.VehicleSQL2
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object QueryController {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val sparkConf = SparkEngine.sparkConf2()
    val sparkSession = SparkEngine.session(sparkConf)

    //获取mongdb数据库的driver_online_record表
    val df: DataFrame = MongoSpark.load(sparkSession)
    df.createOrReplaceTempView("driver_online_record")
//    df.show()

    //------------获取hbase数据库的driver_info表--------------
    val driver_info: DataFrame = readHbase.readHbase_driver_info(sparkConf , sparkSession)
    //创建司机信息表
    driver_info.createOrReplaceTempView("driver_info")

    //本地测试读取mysql数据库的driver_info表
//    val driver_info: DataFrame = sparkSession.read.format("jdbc")
//      .option("url", "jdbc:mysql://47.111.68.2:3306/och_test?useUnicode=true&characterEncoding=utf8")
//      .option("user", "root")
//      .option("password", "123456")
//      .option("dbtable", "driver_info")
//      .option("driver", "com.mysql.jdbc.Driver")
//      .load()


    // ----------------------获取hbase数据库的order_info表---------------------
    val order_info: DataFrame = readHbase.readHbase_order_info(sparkConf , sparkSession)
    //创建订单信息表
    order_info.createOrReplaceTempView("order_info")

    //本地测试读取mysql数据库的order_info表
//    val order_info: DataFrame = sparkSession.read.format("jdbc")
//      .option("url", "jdbc:mysql://47.111.68.2:3306/och_test?useUnicode=true&characterEncoding=utf8")
//      .option("user", "root")
//      .option("password", "123456")
//      .option("dbtable", "order_info")
//      .option("driver", "com.mysql.jdbc.Driver")
//      .load()

    //-----------------------------读取hbase数据库的司管方信息表-----------------------
    val opt_alliance_business = readHbase.readHbase_optAlliance_business(sparkConf , sparkSession)
    //创建司管方信息表
    opt_alliance_business.createOrReplaceTempView("opt_alliance_business")

    //本地测试读取mysql数据库的opt_alliance_business表
//    val opt_alliance_business: DataFrame = sparkSession.read.format("jdbc")
//      .option("url", "jdbc:mysql://47.111.68.2:3306/och_test?useUnicode=true&characterEncoding=utf8")
//      .option("user", "root")
//      .option("password", "123456")
//      .option("dbtable", "opt_alliance_business")
//      .option("driver", "com.mysql.jdbc.Driver")
//      .load()

    //######################统计司机活动时间###################
    sparkSession.sql(VehicleSQL2.driver_online_record_time_rank).createOrReplaceTempView("driver_online_record_time_rank")
    sparkSession.sql(VehicleSQL2.driver_online_record_time_rank).show()
    sparkSession.sql(VehicleSQL2.driver_online_record_time_rank_desc).createOrReplaceTempView("driver_online_record_time_rank_desc")
    sparkSession.sql(VehicleSQL2.driver_online_record_sameday).createOrReplaceTempView("driver_online_record_sameday")
    sparkSession.sql(VehicleSQL2.driver_online_record_yesterday).createOrReplaceTempView("driver_online_record_yesterday")
    sparkSession.sql(VehicleSQL2.driver_online_record_nextday).createOrReplaceTempView("driver_online_record_nextday")
    sparkSession.sql(VehicleSQL2.order_info_sameday).createOrReplaceTempView("order_info_sameday")
    sparkSession.sql(VehicleSQL2.order_info_yesterday).createOrReplaceTempView("order_info_yesterday")
    sparkSession.sql(VehicleSQL2.order_info_nextday).createOrReplaceTempView("order_info_nextday")
    sparkSession.sql(VehicleSQL2.order_info_oneday).createOrReplaceTempView("order_info_oneday")
    sparkSession.sql(VehicleSQL2.driver_info_join_opt_alliance_business).createOrReplaceTempView("driver_info_join_opt_alliance_business")
    sparkSession.sql(VehicleSQL2.order_info_join_driver_info).createOrReplaceTempView("order_info_join_driver_info")
    val frame: DataFrame = sparkSession.sql(VehicleSQL2.order_info_oneday_group1)
    frame.show()



    val url = "jdbc:mysql://47.111.68.2:3306/och_test?useUnicode=true&characterEncoding=utf8"
    val connectionProperties = new Properties()
    connectionProperties.setProperty("user", "root"); // 设置用户名
    connectionProperties.setProperty("password", "123456"); // 设置密码
    frame.write.mode(SaveMode.Append).jdbc(url, "driver_online_time_copy", connectionProperties)


    sparkSession.stop()
    System.exit(0)

  }
}
