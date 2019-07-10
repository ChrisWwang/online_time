package dongfeng.code.tools.spark

import com.typesafe.config.ConfigFactory

/**
  * Created by angel on 2018/11/7.
  */
class GlobalConfigUtils {
  def conf = ConfigFactory.load()
  //开始加载spark相关的配置参数
  def sparkWorkTimeout = conf.getString("spark.worker.timeout")
  def sparkRpcTimeout = conf.getString("spark.rpc.askTimeout")
  def sparkNetWorkTimeout = conf.getString("spark.network.timeoout")
  def sparkMaxCores = conf.getString("spark.cores.max")
  def sparkTaskMaxFailures = conf.getString("spark.task.maxFailures")
  def sparkSpeculation = conf.getString("spark.speculation")
  def sparkAllowMutilpleContext = conf.getString("spark.driver.allowMutilpleContext")
  def sparkSerializer = conf.getString("spark.serializer")
  def sparkBuferSize = conf.getString("spark.buffer.pageSize")
  def initialCharacter = conf.getString("initial.character")
  //Hbase
  def hbaseQuorem = conf.getString("hbase.zookeeper.quorum")
  def hbaseMaster = conf.getString("hbase.master")
  def clientPort = conf.getString("hbase.zookeeper.property.clientPort")
  def rpcTimeout = conf.getString("hbase.rpc.timeout")
  def operatorTimeout = conf.getString("hbase.client.operator.timeout")
  def scannTimeout = conf.getString("hbase.client.scanner.timeout.period")
  //设置时间过滤
  def _begin_time = conf.getString("time.begintime")
  def _over_time = conf.getString("time.overtime")
  //hbase自定义数据源路径
  def customHbasePath = conf.getString("custom.hbase.path")
  //自定义数据源的别名
  def sparksql_table_schema = conf.getString("sparksql_table_schema")
  def hbase_table_name = conf.getString("hbase_table_name")
  def hbase_table_schema = conf.getString("hbase_table_schema")
  def begin_time = conf.getString("begin_time")
  def end_time = conf.getString("end_time")
  //Hbase加载列 - 车辆数据
  def vehicleSparkSQLSchema = conf.getString("vehicleData.sparksql_table_schema")
  def vehicleHbaseSchema = conf.getString("vehicleData.hbase_table_schema")
  def tableVehicleData = conf.getString("table.name.vehicle")
  //Hbase加载列 - 车辆位置数据
  def vehiclePosition_sparksql_schema = conf.getString("vehiclePosition.sparksql_table_schema")
  def vehiclePosition_hbase_schema = conf.getString("vehiclePosition.hbase_table_schema")
  def tableVehiclePosition = conf.getString("table.name.vehiclePosition")
  //Hbase加载列 - 报警信息体
  def AlarmDataInformation_sparksql_schema = conf.getString("AlarmDataInformation.sparksql_table_schema")
  def AlarmDataInformation_hbase_schema = conf.getString("AlarmDataInformation.hbase_table_schema")
  def tableAlarmDataInformation = conf.getString("table.name.AlarmDataInformation")
  //HDFS的namenode地址
  def hdfs = conf.getString("hdfs.host")

  //开始加载es相关配置参数
  def clusterName = conf.getString("cluster.name")
  def autoCreateIndex = conf.getString("es.index.auto.create")
  def esNodes = conf.getString("esNodes")
  def esPort = conf.getString("es.port")
  def isMissing = conf.getString("es.index.reads.missing.as.empty")
  def esNodesDiscovery = conf.getString("es.nodes.discovery")
  def wanOnly = conf.getString("es.nodes.wan.only")
  def esTimeout = conf.getString("es.http.timeout")

  //开始加载MongoDB相关配置参数
  def mongodburi = conf.getString("mongodb.uri")

}

object GlobalConfigUtils extends GlobalConfigUtils
