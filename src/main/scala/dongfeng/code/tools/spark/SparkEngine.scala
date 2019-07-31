package dongfeng.code.tools.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by angel
  */
object SparkEngine {

//  def sparkConf():SparkConf = {
//    val sparkConf: SparkConf = new SparkConf()
//      .set("spark.worker.timeout" , GlobalConfigUtils.sparkWorkTimeout)
//      .set("spark.cores.max" , GlobalConfigUtils.sparkMaxCores)
//      .set("spark.rpc.askTimeout" , GlobalConfigUtils.sparkRpcTimeout)
//      .set("spark.task.macFailures" , GlobalConfigUtils.sparkTaskMaxFailures)
//      .set("spark.speculation" , GlobalConfigUtils.sparkSpeculation)
//      .set("spark.driver.allowMutilpleContext" , GlobalConfigUtils.sparkAllowMutilpleContext)
//      .set("spark.serializer" , GlobalConfigUtils.sparkSerializer)
//      .set("spark.buffer.pageSize" , GlobalConfigUtils.sparkBuferSize)
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.scheduler.mode", "FAIR")
//      .set("spark.mongodb.input.uri", GlobalConfigUtils.mongodburi)
////      .setMaster("local[6]")
//      .setAppName("query")
//
//    sparkConf
//  }

  //sparkconf 配置
  def sparkConf2():SparkConf = {
    val sparkConf: SparkConf = new SparkConf()
      .set("spark.mongodb.input.uri", GlobalConfigUtils.mongodburi)
      .set("spark.sql.adaptive.enabled" , "true")
//      .set("spark.sql.codegen" , "true")
      .set("spark.worker.timeout" , GlobalConfigUtils.sparkWorkTimeout)
      .set("spark.rpc.askTimeout" , GlobalConfigUtils.sparkRpcTimeout)
      .set("spark.task.maxFailures" , GlobalConfigUtils.sparkTaskMaxFailures)
      .set("spark.serializer" , GlobalConfigUtils.sparkSerializer)
      .set("spark.buffer.pageSize" , GlobalConfigUtils.sparkBuferSize)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.cores.max" , GlobalConfigUtils.sparkMaxCores)
//      .set("spark.speculation" , GlobalConfigUtils.sparkSpeculation)
      .set("spark.driver.allowMutilpleContexts" , GlobalConfigUtils.sparkAllowMutilpleContext)
      .set("spark.debug.maxToStringFields", "2000")
//      .setAppName("mytest")
//      .setMaster("local[6]")

    sparkConf//返回值：sparkConf
  }
  //sparksession配置
  def session(sparkConf:SparkConf):SparkSession = {
    val sparkSession: SparkSession = SparkSession.builder()
      .config(sparkConf)
//      .enableHiveSupport() //开启支持hive
      .getOrCreate()
    sparkSession
  }
//  def getSparkSession(sparkConf:SparkConf):SparkSession = {
//    val sparkSession: SparkSession = SparkSession.builder()
//      .config(sparkConf)
////      .enableHiveSupport() //开启支持hive
//      .getOrCreate()
//    sparkSession
//  }
}
