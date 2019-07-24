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
    //  日志等级控制
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    //  获取sparkConf和sparkSession
    val sparkConf = SparkEngine.sparkConf2()
    val sparkSession = SparkEngine.session(sparkConf)


    //--------------------获取mongodb数据库的driver_online_record表---------------------
    val driver_online_record: DataFrame = MongoSpark.load(sparkSession)
    driver_online_record.createOrReplaceTempView("driver_online_record")

    //  另一种获取mongoDB数据的方式
    //        val uri: String = "mongodb://test:test@47.99.187.146:27017/och_test.reservation_order_notified_record_201907"
    //        val pushOrderTb = session.read.format("com.mongodb.spark.sql").options(
    //          Map("spark.mongodb.input.uri" -> uri,
    //            "spark.mongodb.input.partitioner" -> "MongoPaginateBySizePartitioner",
    //            "spark.mongodb.input.partitionerOptions.partitionKey"  -> "_id",
    //            "spark.mongodb.input.partitionerOptions.partitionSizeMB"-> "32"))
    //          .load()
    //        df.show()


    //-----------------------获取hbase数据库的driver_info表--------------------
    //  加载司机表(driver_info)
    var driver_info = sparkSession
      .read
      .format(GlobalConfigUtils.customHbasePath)
      .options(
        Map(
          GlobalConfigUtils.sparksql_table_schema -> GlobalConfigUtils.driverInfo_SparkSQLSchma,
          GlobalConfigUtils.hbase_table_name -> GlobalConfigUtils.table_driver_info,
          GlobalConfigUtils.hbase_table_schema -> GlobalConfigUtils.driverInfo_HbaseSchema
        )).load()
    //  测试读取hbase表
    //  val driver_info: DataFrame = readHbase.readHbase_driver_info(sparkConf , sparkSession)
    //  创建司机信息表
    driver_info.createOrReplaceTempView("driver_info")

    //  本地测试读取mysql数据库的driver_info表
    //    val driver_info: DataFrame = sparkSession.read.format("jdbc")
    //      .option("url", "jdbc:mysql://47.111.68.2:3306/och_test?useUnicode=true&characterEncoding=utf8")
    //      .option("user", "root")
    //      .option("password", "123456")
    //      .option("dbtable", "driver_info")
    //      .option("driver", "com.mysql.jdbc.Driver")
    //      .load()
    //    driver_info.show()


    // ----------------------获取hbase数据库的order_info表---------------------
    var order_info = sparkSession
      .read
      .format(GlobalConfigUtils.customHbasePath)
      .options(
        Map(
          GlobalConfigUtils.sparksql_table_schema -> GlobalConfigUtils.orderInfo_SparkSQLSchma,
          GlobalConfigUtils.hbase_table_name -> GlobalConfigUtils.table_order_info,
          GlobalConfigUtils.hbase_table_schema -> GlobalConfigUtils.orderInfo_HbaseSchema
        )).load()
    //  测试读取hbase表
    //  val order_info: DataFrame = readHbase.readHbase_order_info(sparkConf , sparkSession)
    //  创建订单信息表
    order_info.createOrReplaceTempView("order_info")

    //  本地测试读取mysql数据库的order_info表
    //    val order_info: DataFrame = sparkSession.read.format("jdbc")
    //      .option("url", "jdbc:mysql://47.111.68.2:3306/och_test?useUnicode=true&characterEncoding=utf8")
    //      .option("user", "root")
    //      .option("password", "123456")
    //      .option("dbtable", "order_info")
    //      .option("driver", "com.mysql.jdbc.Driver")
    //      .load()
    //    order_info.show()


    //---------------------------读取hbase数据库的司管方信息表-----------------------
    var opt_alliance_business = sparkSession
      .read
      .format(GlobalConfigUtils.customHbasePath)
      .options(
        Map(
          GlobalConfigUtils.sparksql_table_schema -> GlobalConfigUtils.alliance_business_SparkSQLSchema,
          GlobalConfigUtils.hbase_table_name -> GlobalConfigUtils.table_alliance_business,
          GlobalConfigUtils.hbase_table_schema -> GlobalConfigUtils.alliance_business_HbaseSchema
        )).load()
    //  测试读取hbase表
    //  val opt_alliance_business = readHbase.readHbase_optAlliance_business(sparkConf , sparkSession)
    //  创建司管方信息表
    opt_alliance_business.createOrReplaceTempView("opt_alliance_business")

    //  本地测试读取mysql数据库的opt_alliance_business表
    //    val opt_alliance_business: DataFrame = sparkSession.read.format("jdbc")
    //      .option("url", "jdbc:mysql://47.111.68.2:3306/och_test?useUnicode=true&characterEncoding=utf8")
    //      .option("user", "root")
    //      .option("password", "123456")
    //      .option("dbtable", "opt_alliance_business")
    //      .option("driver", "com.mysql.jdbc.Driver")
    //      .load()
    //    opt_alliance_business.show()


    //######################统计司机活动时间###################
    sparkSession.sql(VehicleSQL2.driver_online_record_time_all).createOrReplaceTempView("driver_online_record_time_all")
    sparkSession.sql(VehicleSQL2.driver_online_record_time_filter).createOrReplaceTempView("driver_online_record_time_filter")
    sparkSession.sql(VehicleSQL2.driver_online_record_time_rank).createOrReplaceTempView("driver_online_record_time_rank")
    sparkSession.sql(VehicleSQL2.driver_online_record_time_rank_desc).createOrReplaceTempView("driver_online_record_time_rank_desc")
    //----------------------------------------------------------------------------------
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


    //    测试数据库
    //    val url = "jdbc:mysql://120.27.208.185:3306/och_test?useUnicode=true&characterEncoding=utf8"
    //    val connectionProperties = new Properties()
    //    connectionProperties.setProperty("user", "och_test"); // 设置用户名
    //    connectionProperties.setProperty("password", "Kop21IQ8"); // 设置密码
    //    frame.write.mode(SaveMode.Append).jdbc(url, "driver_online_time", connectionProperties)


    //    高质数据库
    //    val url = "jdbc:mysql://47.111.68.2:3306/och_test?useUnicode=true&characterEncoding=utf8"
    //    val connectionProperties = new Properties()
    //    connectionProperties.setProperty("user", "root"); // 设置用户名
    //    connectionProperties.setProperty("password", "123456"); // 设置密码
    //    connectionProperties.setProperty("driver", "com.mysql.jdbc.Driver")
    //    frame.write.mode(SaveMode.Append).jdbc(url, "driver_online_time_copy", connectionProperties)


    //    开发mysql数据库（未提供）
    //    val url = "jdbc:mysql://120.27.208.185:3306/och_test?useUnicode=true&characterEncoding=utf8"
    //    val connectionProperties = new Properties()
    //    connectionProperties.setProperty("user", "och_test"); // 设置用户名
    //    connectionProperties.setProperty("password", "Kop21IQ8"); // 设置密码
    //    frame.write.mode(SaveMode.Append).jdbc(url, "driver_online_time", connectionProperties)

    //  释放资源
    sparkSession.stop()
    System.exit(0)

  }
}
