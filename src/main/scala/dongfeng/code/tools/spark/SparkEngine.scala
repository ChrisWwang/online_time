package dongfeng.code.tools.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by angel
  */
object SparkEngine {

  def sparkConf():SparkConf = {
    val sparkConf: SparkConf = new SparkConf()
      .set("spark.worker.timeout" , GlobalConfigUtils.sparkWorkTimeout)
      .set("spark.cores.max" , GlobalConfigUtils.sparkMaxCores)
      .set("spark.rpc.askTimeout" , GlobalConfigUtils.sparkRpcTimeout)
      .set("spark.task.macFailures" , GlobalConfigUtils.sparkTaskMaxFailures)
      .set("spark.speculation" , GlobalConfigUtils.sparkSpeculation)
      .set("spark.driver.allowMutilpleContext" , GlobalConfigUtils.sparkAllowMutilpleContext)
      .set("spark.serializer" , GlobalConfigUtils.sparkSerializer)
      .set("spark.buffer.pageSize" , GlobalConfigUtils.sparkBuferSize)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.scheduler.mode", "FAIR")
      .set("cluster.name" , GlobalConfigUtils.clusterName)
      .set("es.index.auto.create" , GlobalConfigUtils.autoCreateIndex)
      .set("es.nodes" , GlobalConfigUtils.esNodes)
      .set("es.port" , GlobalConfigUtils.esPort)
      .set("es.index.reads.missing.as.empty" , GlobalConfigUtils.isMissing)
      .set("es.nodes.discovery" , GlobalConfigUtils.esNodesDiscovery)
      .set("es.nodes.wan.only" , GlobalConfigUtils.wanOnly)
      .set("es.http.timeout" , GlobalConfigUtils.esTimeout)
      .set("spark.mongodb.input.uri","mongodb://127.0.0.1/test.user")
      .setMaster("local[6]")
      .setAppName("query")

    sparkConf
  }

  def sparkConf2():SparkConf = {
    val sparkConf: SparkConf = new SparkConf()
      .set("spark.mongodb.input.uri", GlobalConfigUtils.mongodburi)
      .setAppName("mytest")
      .setMaster("local[6]")

    sparkConf//返回值：sparkConf
  }
  def session(sparkConf:SparkConf):SparkSession = {
    val sparkSession: SparkSession = SparkSession.builder()
      .config(sparkConf)
//      .enableHiveSupport() //开启支持hive
      .getOrCreate()
    sparkSession
  }
}
