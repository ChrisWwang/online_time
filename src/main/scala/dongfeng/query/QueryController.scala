package dongfeng.query

import java.util.Properties

import com.mongodb.spark.MongoSpark
import dongfeng.code.tools.spark.SparkEngine
import dongfeng.query.sql.VehicleSQL
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode}

object QueryController {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)


    val sparkConf = SparkEngine.sparkConf()
      .set("spark.mongodb.input.uri", "mongodb://test:test@47.99.187.146/och_test.driver_online_record")

    val sparkSession = SparkEngine.session(sparkConf)
    import sparkSession.sqlContext.implicits._


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
    //      createOrReplaceTempView("driver_info")

    // 外联车辆信息表
    val order_info: DataFrame = sparkSession.read.format("jdbc")
      .option("url", "jdbc:mysql://47.111.68.2:3306/och_test?useUnicode=true&characterEncoding=utf8")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "order_info_201906")
      .option("driver", "com.mysql.jdbc.Driver")
      .load()
    //创建车辆信息表
    order_info.createOrReplaceTempView("order_info")
    //      createOrReplaceTempView("order_info")


    val opt_alliance_business: DataFrame = sparkSession.read.format("jdbc")
      .option("url", "jdbc:mysql://47.111.68.2:3306/och_test?useUnicode=true&characterEncoding=utf8")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "opt_alliance_business")
      .option("driver", "com.mysql.jdbc.Driver")
      .load()
    //创建车辆信息表
    opt_alliance_business.createOrReplaceTempView("opt_alliance_business")
    //      createOrReplaceTempView("driver_info")


    //######################今日三级故障数###################
    sparkSession.sql(VehicleSQL.order_info_sameday).createOrReplaceTempView("order_info_sameday")
    sparkSession.sql(VehicleSQL.order_info_yesterday).createOrReplaceTempView("order_info_yesterday")
    sparkSession.sql(VehicleSQL.order_info_nextday).createOrReplaceTempView("order_info_nextday")
    sparkSession.sql(VehicleSQL.order_info_oneday).createOrReplaceTempView("order_info_oneday")
    sparkSession.sql(VehicleSQL.driver_info_join_opt_alliance_business).createOrReplaceTempView("driver_info_join_opt_alliance_business")
    sparkSession.sql(VehicleSQL.order_info_join_driver_info).createOrReplaceTempView("order_info_join_driver_info")
    //    sparkSession.sql(VehicleSQL.order_info_oneday_group1).show()
    val df1: DataFrame = sparkSession.sql(VehicleSQL.order_info_oneday_group1)
    df1.show()
    //          createOrReplaceTempView("order_info_oneday_group")
    //    val df: DataFrame = sparkSession.sql(HuiVehicleSQL.order_info_5min_1)

    //    sparkSession.sql(HuiVehicleSQL.order_info_sameday_group).show()

    val url = "jdbc:mysql://120.27.208.185:3306/och_test?useUnicode=true&characterEncoding=utf8"
    val connectionProperties = new Properties()
    connectionProperties.setProperty("user", "och_test"); // 设置用户名
    connectionProperties.setProperty("password", "Kop21IQ8"); // 设置密码
    df1.write.mode(SaveMode.Append).jdbc(url, "driver_online_time", connectionProperties)


    sparkSession.stop()
    System.exit(0)

  }
}
