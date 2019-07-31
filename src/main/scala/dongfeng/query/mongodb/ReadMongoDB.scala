package dongfeng.query.mongodb

import com.mongodb.spark.MongoSpark
import dongfeng.code.tools.spark.SparkEngine
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


object ReadMongoDB {

  def main(args: Array[String]): Unit = {

    val sparkconf : SparkConf = SparkEngine.sparkConf2()

    val sparksession = SparkEngine.session(sparkconf)
    // 设置log级别
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val df:DataFrame = MongoSpark.load(sparksession)
    df.show()

    df.createOrReplaceTempView("user")

//    val resDf:DataFrame = sparksession.sql("select  driverId, createTime, state from user")
//    resDf.show()

    sparksession.stop()
    System.exit(0)
  }
}
