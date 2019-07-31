package dongfeng.query

import java.util.Properties

import com.mongodb.spark.MongoSpark
import dongfeng.code.tools.spark.{GlobalConfigUtils, SparkEngine}
import dongfeng.query.begin.{MongoBegin, MySqlBegin}
import dongfeng.query.hbase.readHbase
import dongfeng.query.process._
import dongfeng.query.sql.{OtherDayVehicleSQL2, OtherDayVehicleSQL3, VehicleSQL2}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object QueryController {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val sparkConf = SparkEngine.sparkConf2()
    val sparkSession = SparkEngine.session(sparkConf)


    //--------------------------获取mongdb数据库的driver_online_record表-------------------
//    val driver_online_record: DataFrame = MongoSpark.load(sparkSession).repartition(1)
//    driver_online_record.createOrReplaceTempView("driver_online_record")
    //

    //--------------------------获取hbase数据库的driver_info表--------------------
    //TODO 加载司机表(driver_info) 需打开
//        var driver_info = sparkSession
//          .read
//          .format(GlobalConfigUtils.customHbasePath)
//          .options(
//            Map(
//              GlobalConfigUtils.sparksql_table_schema -> GlobalConfigUtils.driverInfo_SparkSQLSchma,
//              GlobalConfigUtils.hbase_table_name -> GlobalConfigUtils.table_driver_info,
//              GlobalConfigUtils.hbase_table_schema -> GlobalConfigUtils.driverInfo_HbaseSchema
//            )).load()
//        val driver_info: DataFrame = readHbase.readHbase_driver_info(sparkConf , sparkSession)

    //    driver_info.show()
    //    //<<<<<预发布环境>>>>>读取mysql数据库的driver_info表  本地测试需要修改为>>>>>>>>外网ip<<<<<<<<<
    //    val driver_info: DataFrame = sparkSession.read.format("jdbc")
    //      .option("url", "jdbc:mysql://10.0.12.192:3306/och_ci?useUnicode=true&characterEncoding=utf8")
    //      .option("user", "och_ci")
    //      .option("password", "59whJLdQ")
    //      .option("dbtable", "driver_info")
    //      .option("driver", "com.mysql.jdbc.Driver")
    //      .load()

    //    //<<<<<测试环境>>>>>读取mysql数据库的driver_info表
    //    val driver_info: DataFrame = sparkSession.read.format("jdbc")
    //      .option("url", "jdbc:mysql://120.27.208.185:3306/och_test?useUnicode=true&characterEncoding=utf8")
    //      .option("user", "och_test")
    //      .option("password", "Kop21IQ8")
    //      .option("dbtable", "driver_info")
    //      .option("driver", "com.mysql.jdbc.Driver")
    //      .load()

    //TODO （生产时需打开）创建司机信息表
//        driver_info.createOrReplaceTempView("driver_info")



    // ----------------------获取hbase数据库的order_info表---------------------
    //    TODO （生产时需打开）
    //    var order_info = sparkSession
    //      .read
    //      .format(GlobalConfigUtils.customHbasePath)
    //      .options(
    //        Map(
    //          GlobalConfigUtils.sparksql_table_schema -> GlobalConfigUtils.orderInfo_SparkSQLSchma,
    //          GlobalConfigUtils.hbase_table_name -> GlobalConfigUtils.table_order_info,
    //          GlobalConfigUtils.hbase_table_schema -> GlobalConfigUtils.orderInfo_HbaseSchema
    //        )).load()
    //    val order_info: DataFrame = readHbase.readHbase_order_info(sparkConf , sparkSession)

    //    //<<<<<预发布环境>>>>>读取mysql数据库的order_info表
    //    val order_info: DataFrame = sparkSession.read.format("jdbc")
    //      .option("url", "jdbc:mysql://10.0.12.192:3306/och_ci?useUnicode=true&characterEncoding=utf8")
    //      .option("user", "och_ci")
    //      .option("password", "59whJLdQ")
    //      .option("dbtable", "order_info_201907")
    //      .option("driver", "com.mysql.jdbc.Driver")
    //      .load()

    //    //<<<<<测试环境>>>>>读取mysql数据库的order_info表
    //    val order_info: DataFrame = sparkSession.read.format("jdbc")
    //      .option("url", "jdbc:mysql://120.27.208.185:3306/och_test?useUnicode=true&characterEncoding=utf8")
    //      .option("user", "och_test")
    //      .option("password", "Kop21IQ8")
    //      .option("dbtable", "order_info_201907")
    //      .option("driver", "com.mysql.jdbc.Driver")
    //      .load()

    //创建订单信息表
    //    TODO （生产时需打开）
    //    order_info.createOrReplaceTempView("order_info")

    // TODO -----------------------这个是测试 切记-------------------------------
    //<<<<<生产环境>>>>>读取mysql数据库的order_info表
//    val order_info: DataFrame = sparkSession.read.format("jdbc")
//      .option("url", "jdbc:mysql://10.0.15.145:3306/och_prd?useUnicode=true&characterEncoding=utf8")
//      .option("user", "och_prd")
//      .option("password", "59whJDFQ")
//      .option("dbtable", "order_info_201907")
//      .option("driver", "com.mysql.jdbc.Driver")
//      .load()
//    order_info.createOrReplaceTempView("order_info")

    //<<<<<生产环境>>>>>读取mysql数据库的driver_info表
//    val driver_info: DataFrame = sparkSession.read.format("jdbc")
//      .option("url", "jdbc:mysql://10.0.15.145:3306/och_prd?useUnicode=true&characterEncoding=utf8")
//      .option("user", "och_prd")
//      .option("password", "59whJDFQ")
//      .option("dbtable", "driver_info")
//      .option("driver", "com.mysql.jdbc.Driver")
//      .load()
//    driver_info.createOrReplaceTempView("driver_info")

    //<<<<<生产环境>>>>>读取mysql数据库的opt_alliance_business表
//    val opt_alliance_business: DataFrame = sparkSession.read.format("jdbc")
//      .option("url", "jdbc:mysql://10.0.15.145:3306/och_prd?useUnicode=true&characterEncoding=utf8")
//      .option("user", "och_prd")
//      .option("password", "59whJDFQ")
//      .option("dbtable", "opt_alliance_business")
//      .option("driver", "com.mysql.jdbc.Driver")
//      .load()
//    opt_alliance_business.createOrReplaceTempView("opt_alliance_business")

    //TODO ----------------------------------------------------------------------------






    //-----------------------------读取hbase数据库的司管方信息表-----------------------
    //    TODO （生产时需打开）
    //    var opt_alliance_business = sparkSession
    //      .read
    //      .format(GlobalConfigUtils.customHbasePath)
    //      .options(
    //        Map(
    //          GlobalConfigUtils.sparksql_table_schema -> GlobalConfigUtils.alliance_business_SparkSQLSchema,
    //          GlobalConfigUtils.hbase_table_name -> GlobalConfigUtils.table_alliance_business,
    //          GlobalConfigUtils.hbase_table_schema -> GlobalConfigUtils.alliance_business_HbaseSchema
    //        )).load()
    //    val opt_alliance_business = readHbase.readHbase_optAlliance_business(sparkConf , sparkSession)

    //    //<<<<<预发布环境>>>>>读取mysql数据库的opt_alliance_business表
//        val opt_alliance_business: DataFrame = sparkSession.read.format("jdbc")
//          .option("url", "jdbc:mysql://10.0.12.192:3306/och_ci?useUnicode=true&characterEncoding=utf8")
//          .option("user", "och_ci")
//          .option("password", "59whJLdQ")
//          .option("dbtable", "opt_alliance_business")
//          .option("driver", "com.mysql.jdbc.Driver")
//          .load()

    //    //<<<<<测试环境>>>>>读取mysql数据库的opt_alliance_business表
//        val opt_alliance_business: DataFrame = sparkSession.read.format("jdbc")
//          .option("url", "jdbc:mysql://120.27.208.185:3306/och_test?useUnicode=true&characterEncoding=utf8")
//          .option("user", "och_test")
//          .option("password", "Kop21IQ8")
//          .option("dbtable", "opt_alliance_business")
//          .option("driver", "com.mysql.jdbc.Driver")
//          .load()

    //创建司管方信息表
    //    TODO （生产时需打开）
    //    opt_alliance_business.createOrReplaceTempView("opt_alliance_business")

    //#################################################统计司机活动时间##################################################

    //    //---------------修改 driver_id 、 create_time 相等切同时 state = 1 和 0 和 0 ----------------
    //    sparkSession.sql(VehicleSQL2.driver_online_record_time_original).createOrReplaceTempView("driver_online_record_time_original")
    //    sparkSession.sql(VehicleSQL2.driver_online_record_time_state_0).createOrReplaceTempView("driver_online_record_time_state_0")
    //    sparkSession.sql(VehicleSQL2.driver_online_record_time_state_1).createOrReplaceTempView("driver_online_record_time_state_1")
    //    //--------------------------------------------------------------------------------------------
    //
    //    //---------------修改 driver_id 、 create_time 相等切同时 state = 1 和 0----------------------
    //
    //    sparkSession.sql(VehicleSQL2.driver_online_record_time_all).createOrReplaceTempView("driver_online_record_time_all")
    //    sparkSession.sql(VehicleSQL2.driver_online_record_time_filter).createOrReplaceTempView("driver_online_record_time_filter")
    //    sparkSession.sql(VehicleSQL2.driver_online_record_time_rank).createOrReplaceTempView("driver_online_record_time_rank")
    //    sparkSession.sql(VehicleSQL2.driver_online_record_time_rank_desc).createOrReplaceTempView("driver_online_record_time_rank_desc")
    //    //-----------------------------------------------------------------------------------
    //    sparkSession.sql(VehicleSQL2.driver_online_record_sameday).createOrReplaceTempView("driver_online_record_sameday")
    //    sparkSession.sql(VehicleSQL2.driver_online_record_yesterday).createOrReplaceTempView("driver_online_record_yesterday")
    //    sparkSession.sql(VehicleSQL2.driver_online_record_nextday).createOrReplaceTempView("driver_online_record_nextday")
    //    sparkSession.sql(VehicleSQL2.order_info_sameday).createOrReplaceTempView("order_info_sameday")
    //    sparkSession.sql(VehicleSQL2.order_info_yesterday).createOrReplaceTempView("order_info_yesterday")
    //    sparkSession.sql(VehicleSQL2.order_info_nextday).createOrReplaceTempView("order_info_nextday")
    //    sparkSession.sql(VehicleSQL2.order_info_oneday).createOrReplaceTempView("order_info_oneday")
    //    sparkSession.sql(VehicleSQL2.driver_info_join_opt_alliance_business).createOrReplaceTempView("driver_info_join_opt_alliance_business")
    //    sparkSession.sql(VehicleSQL2.order_info_join_driver_info).createOrReplaceTempView("order_info_join_driver_info")
    //    val frame: DataFrame = sparkSession.sql(VehicleSQL2.order_info_oneday_group1)
    //    frame.show()
    //#################################################################################################################
//    sparkSession.sql(OtherDayVehicleSQL2.driver_online_record_time_original).createOrReplaceTempView("driver_online_record_time_original")
//    sparkSession.sql(OtherDayVehicleSQL2.driver_online_record_time_state_0).createOrReplaceTempView("driver_online_record_time_state_0")
//    sparkSession.sql(OtherDayVehicleSQL2.driver_online_record_time_state_1).createOrReplaceTempView("driver_online_record_time_state_1")
//    sparkSession.sql(OtherDayVehicleSQL2.driver_online_record_time_all).createOrReplaceTempView("driver_online_record_time_all")
//    sparkSession.sql(OtherDayVehicleSQL2.driver_online_record_time_filter).createOrReplaceTempView("driver_online_record_time_filter")
//    sparkSession.sql(OtherDayVehicleSQL2.driver_online_record_time_rank).createOrReplaceTempView("driver_online_record_time_rank")
//    sparkSession.sql(OtherDayVehicleSQL2.driver_online_record_time_rank_desc).createOrReplaceTempView("driver_online_record_time_rank_desc")
//    sparkSession.sql(OtherDayVehicleSQL2.driver_online_record_sameday).createOrReplaceTempView("driver_online_record_sameday")
//    sparkSession.sql(OtherDayVehicleSQL2.driver_online_record_sameday).show(1000)
//    sparkSession.sql(OtherDayVehicleSQL2.driver_online_record_yesterday).createOrReplaceTempView("driver_online_record_yesterday")
//    sparkSession.sql(OtherDayVehicleSQL2.driver_online_record_nextday).createOrReplaceTempView("driver_online_record_nextday")
//    sparkSession.sql(OtherDayVehicleSQL2.order_info_sameday).createOrReplaceTempView("order_info_sameday")
//    sparkSession.sql(OtherDayVehicleSQL2.order_info_yesterday).createOrReplaceTempView("order_info_yesterday")
//    sparkSession.sql(OtherDayVehicleSQL2.order_info_nextday).createOrReplaceTempView("order_info_nextday")
//    sparkSession.sql(OtherDayVehicleSQL2.order_info_oneday).createOrReplaceTempView("order_info_oneday")
//    sparkSession.sql(OtherDayVehicleSQL2.driver_info_join_opt_alliance_business).createOrReplaceTempView("driver_info_join_opt_alliance_business")
//    sparkSession.sql(OtherDayVehicleSQL2.order_info_join_driver_info).createOrReplaceTempView("order_info_join_driver_info")
//    sparkSession.sql(OtherDayVehicleSQL2.order_info_join_driver_info).show(1000)
//    val frame: DataFrame = sparkSession.sql(OtherDayVehicleSQL2.order_info_oneday_group1)
    //#################################################################################################################


//    val v1 = sparkSession.sql(OtherDayVehicleSQL2.driver_online_record_time_original).repartition(1)
//    v1.createOrReplaceTempView("driver_online_record_time_original")
//    val v2 = sparkSession.sql(OtherDayVehicleSQL2.driver_online_record_time_state_0).repartition(1)
//    v2.createOrReplaceTempView("driver_online_record_time_state_0")
//    val v3 = sparkSession.sql(OtherDayVehicleSQL2.driver_online_record_time_state_1).repartition(1)
//    v3.createOrReplaceTempView("driver_online_record_time_state_1")
//    val v4 = sparkSession.sql(OtherDayVehicleSQL2.driver_online_record_time_all).repartition(1)
//    v4.createOrReplaceTempView("driver_online_record_time_all")
//    val v5 = sparkSession.sql(OtherDayVehicleSQL2.driver_online_record_time_filter).repartition(1)
//    v5.createOrReplaceTempView("driver_online_record_time_filter")
//    val v6 = sparkSession.sql(OtherDayVehicleSQL2.driver_online_record_time_rank).repartition(1)
//    v6.createOrReplaceTempView("driver_online_record_time_rank")
//    val v7 = sparkSession.sql(OtherDayVehicleSQL2.driver_online_record_time_rank_desc).repartition(1)
//    v7.createOrReplaceTempView("driver_online_record_time_rank_desc")
//    val v8 = sparkSession.sql(OtherDayVehicleSQL2.driver_online_record_sameday).repartition(1)
//    v8.createOrReplaceTempView("driver_online_record_sameday")
////    sparkSession.sql(OtherDayVehicleSQL2.driver_online_record_sameday).show(1000)
//    val v9 = sparkSession.sql(OtherDayVehicleSQL2.driver_online_record_yesterday).repartition(1)
//    v9.createOrReplaceTempView("driver_online_record_yesterday")
//    val v10 = sparkSession.sql(OtherDayVehicleSQL2.driver_online_record_nextday).repartition(1)
//    v10.createOrReplaceTempView("driver_online_record_nextday")
//    val v11 = sparkSession.sql(OtherDayVehicleSQL2.order_info_sameday).repartition(1)
//    v11.createOrReplaceTempView("order_info_sameday")
//    val v12 = sparkSession.sql(OtherDayVehicleSQL2.order_info_yesterday).repartition(1)
//    v12.createOrReplaceTempView("order_info_yesterday")
//    val v13 = sparkSession.sql(OtherDayVehicleSQL2.order_info_nextday).repartition(1)
//    v13.createOrReplaceTempView("order_info_nextday")
//    val v14 = sparkSession.sql(OtherDayVehicleSQL2.order_info_oneday).repartition(1)
//    v14.createOrReplaceTempView("order_info_oneday")
//    val v15 = sparkSession.sql(OtherDayVehicleSQL2.driver_info_join_opt_alliance_business).repartition(1)
//    v15.createOrReplaceTempView("driver_info_join_opt_alliance_business")
//    val v16 = sparkSession.sql(OtherDayVehicleSQL2.order_info_join_driver_info).repartition(1)
//    v16.createOrReplaceTempView("order_info_join_driver_info")
////    sparkSession.sql(OtherDayVehicleSQL2.order_info_join_driver_info).show(1000)
//    val frame: DataFrame = sparkSession.sql(OtherDayVehicleSQL2.order_info_oneday_group1).repartition(1)



//    sparkSession.sql(OtherDayVehicleSQL3.driver_online_record_time_original).createOrReplaceTempView("driver_online_record_time_original")
//    sparkSession.sql(OtherDayVehicleSQL3.order_info_filter).createOrReplaceTempView("order_info_filter")
//    sparkSession.sql(OtherDayVehicleSQL3.driver_online_record_time_state_0).createOrReplaceTempView("driver_online_record_time_state_0")
//    sparkSession.sql(OtherDayVehicleSQL3.driver_online_record_time_state_1).createOrReplaceTempView("driver_online_record_time_state_1")
//    sparkSession.sql(OtherDayVehicleSQL3.driver_online_record_time_all).createOrReplaceTempView("driver_online_record_time_all")
//    sparkSession.sql(OtherDayVehicleSQL3.driver_online_record_time_filter).createOrReplaceTempView("driver_online_record_time_filter")
//    sparkSession.sql(OtherDayVehicleSQL3.driver_online_record_time_rank).createOrReplaceTempView("driver_online_record_time_rank")
//    sparkSession.sql(OtherDayVehicleSQL3.driver_online_record_time_rank_desc).createOrReplaceTempView("driver_online_record_time_rank_desc")
//    sparkSession.sql(OtherDayVehicleSQL3.driver_online_record_sameday).createOrReplaceTempView("driver_online_record_sameday")
//    sparkSession.sql(OtherDayVehicleSQL3.driver_online_record_yesterday).createOrReplaceTempView("driver_online_record_yesterday")
//    sparkSession.sql(OtherDayVehicleSQL3.driver_online_record_nextday).createOrReplaceTempView("driver_online_record_nextday")
//    sparkSession.sql(OtherDayVehicleSQL3.order_info_sameday).createOrReplaceTempView("order_info_sameday")
//    sparkSession.sql(OtherDayVehicleSQL3.order_info_yesterday).createOrReplaceTempView("order_info_yesterday")
//    sparkSession.sql(OtherDayVehicleSQL3.order_info_nextday).createOrReplaceTempView("order_info_nextday")
//    sparkSession.sql(OtherDayVehicleSQL3.order_info_oneday).createOrReplaceTempView("order_info_oneday")
//    sparkSession.sql(OtherDayVehicleSQL3.driver_info_join_opt_alliance_business).createOrReplaceTempView("driver_info_join_opt_alliance_business")
//    sparkSession.sql(OtherDayVehicleSQL3.order_info_join_driver_info).createOrReplaceTempView("order_info_join_driver_info")
//    val frame: DataFrame = sparkSession.sql(OtherDayVehicleSQL3.order_info_oneday_group1)
//    frame.show()


    // mongo
    val driver_online_mongo_oneday = MongoBegin.mongo_begin_onlinetime(sparkSession)

    driver_online_mongo_oneday.createOrReplaceTempView("driver_online_mongo_oneday")

    // mysql
    val driver_online_mysql_oneday = MySqlBegin.mysql_begin_onlinetime(sparkSession)

    driver_online_mysql_oneday.createOrReplaceTempView("driver_online_mysql_oneday")


    // 合并
    sparkSession.sql(OtherDayVehicleSQL3.driver_online_oneday).createOrReplaceTempView("driver_online_oneday")



    //<<<<<生产环境>>>>>读取mysql数据库的driver_info表
    val driver_info: DataFrame = sparkSession.read.format("jdbc")
      .option("url", "jdbc:mysql://10.0.15.145:3306/och_prd?useUnicode=true&characterEncoding=utf8")
      .option("user", "och_prd")
      .option("password", "59whJDFQ")
      .option("dbtable", "driver_info")
      .option("driver", "com.mysql.jdbc.Driver")
      .load()
    driver_info.createOrReplaceTempView("driver_info")

    //<<<<<生产环境>>>>>读取mysql数据库的opt_alliance_business表
    val opt_alliance_business: DataFrame = sparkSession.read.format("jdbc")
      .option("url", "jdbc:mysql://10.0.15.145:3306/och_prd?useUnicode=true&characterEncoding=utf8")
      .option("user", "och_prd")
      .option("password", "59whJDFQ")
      .option("dbtable", "opt_alliance_business")
      .option("driver", "com.mysql.jdbc.Driver")
      .load()
    opt_alliance_business.createOrReplaceTempView("opt_alliance_business")


    sparkSession.sql(OtherDayVehicleSQL3.driver_info_join_opt_alliance_business).createOrReplaceTempView("driver_info_join_opt_alliance_business")
    sparkSession.sql(OtherDayVehicleSQL3.order_info_join_driver_info).createOrReplaceTempView("order_info_join_driver_info")
    val frame: DataFrame = sparkSession.sql(OtherDayVehicleSQL3.order_info_oneday_group1)
    frame.show()







    //----------------------------写数据-----------------------------
    //      写入高芝测试数据库(大数据)
    val url = "jdbc:mysql://47.111.68.2:3306/och_test?useUnicode=true&characterEncoding=utf8"
    val connectionProperties = new Properties()
    connectionProperties.setProperty("user", "root"); // 设置用户名
    connectionProperties.setProperty("password", "123456"); // 设置密码
    connectionProperties.setProperty("driver", "com.mysql.jdbc.Driver")
    frame.write.mode(SaveMode.Append).jdbc(url, "driver_online_time_copy_yzq_729", connectionProperties)


    //    //写入<<<<<生产环境>>>>>数据库
//            val url = "jdbc:mysql://10.0.15.145:3306/och_prd?useUnicode=true&characterEncoding=utf8"
//            val connectionProperties = new Properties()
//            connectionProperties.setProperty("user", "och_prd"); // 设置用户名
//            connectionProperties.setProperty("password", "59whJDFQ"); // 设置密码
//            connectionProperties.setProperty("driver", "com.mysql.jdbc.Driver")
//            frame.write.mode(SaveMode.Append).jdbc(url, "driver_online_time", connectionProperties)



    //    //写入服务端测试数据库
    //    val url = "jdbc:mysql://120.27.208.185:3306/och_test?useUnicode=true&characterEncoding=utf8"
    //    val connectionProperties = new Properties()
    //    connectionProperties.setProperty("user", "och_test"); // 设置用户名
    //    connectionProperties.setProperty("password", "Kop21IQ8"); // 设置密码
    //    connectionProperties.setProperty("driver", "com.mysql.jdbc.Driver")
    //    frame.write.mode(SaveMode.Append).jdbc(url, "driver_online_time", connectionProperties)

    //    //    //写入<<<<<预发布环境>>>>>数据库
    //        val url = "jdbc:mysql://10.0.12.192:3306/och_ci?useUnicode=true&characterEncoding=utf8"
    //        val connectionProperties = new Properties()
    //        connectionProperties.setProperty("user", "och_ci"); // 设置用户名
    //        connectionProperties.setProperty("password", "59whJLdQ"); // 设置密码
    //        connectionProperties.setProperty("driver", "com.mysql.jdbc.Driver")
    //        frame.write.mode(SaveMode.Append).jdbc(url, "driver_online_time", connectionProperties)



    sparkSession.stop()
    System.exit(0)

  }
}
