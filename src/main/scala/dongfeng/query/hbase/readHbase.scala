package dongfeng.query.hbase

import java.util.Properties

import dongfeng.code.tools.spark.{GlobalConfigUtils, SparkEngine}
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
object readHbase {
  def main(args:Array[String]):Unit = {
    val sparkConf = SparkEngine.sparkConf2()
    val sparkSession = SparkEngine.session(sparkConf)
    readHbase_driver_info(sparkConf , sparkSession)
  }

  //读取hbase数据库的司管方信息表（opt_alliance_business）
  def readHbase_optAlliance_business(sparkConf : SparkConf , sparkSession : SparkSession): DataFrame = {
    val sc = sparkSession.sparkContext
    //###########用于使用  toDF   ###############
    val sqlContext = SparkSession.builder().config(sparkConf).getOrCreate().sqlContext
    import sqlContext.implicits._
    //###########################################
    //初始化HBaseConfiguration
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", GlobalConfigUtils.hbaseQuorem)    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    hbaseConf.set("hbase.zookeeper.property.clientPort", GlobalConfigUtils.clientPort)  //设置zookeeper连接端口，默认2181
    hbaseConf.set(TableInputFormat.INPUT_TABLE, GlobalConfigUtils.table_alliance_business) //设置要读取hbase的表名

    //读取数据并转化成rdd
    val hBaseRDD = sc.newAPIHadoopRDD(
        hbaseConf,
        classOf[TableInputFormat],
        classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
        classOf[org.apache.hadoop.hbase.client.Result])

    //将rdd装换为dataframe
    val opt_alliance_business : DataFrame = hBaseRDD.map(r=>(
       Bytes.toString(r._2.getValue(Bytes.toBytes("MM"),Bytes.toBytes("id_"))),
       Bytes.toString(r._2.getValue(Bytes.toBytes("MM"),Bytes.toBytes("alliance_name")))
     )).toDF("id_","alliance_name")

    //显示读取到的hbase表
//    opt_alliance_business.show()

    opt_alliance_business
  }

  //读取hbase数据库的order_info表（订单信息表）
  def readHbase_order_info(sparkConf : SparkConf , sparkSession : SparkSession): DataFrame = {
    val sc = sparkSession.sparkContext
    //###########用于使用  toDF   ###############
    val sqlContext = SparkSession.builder().config(sparkConf).getOrCreate().sqlContext
    import sqlContext.implicits._
    //###########################################
    //初始化HBaseConfiguration
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", GlobalConfigUtils.hbaseQuorem)    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    hbaseConf.set("hbase.zookeeper.property.clientPort", GlobalConfigUtils.clientPort)  //设置zookeeper连接端口，默认2181
    hbaseConf.set(TableInputFormat.INPUT_TABLE, GlobalConfigUtils.table_order_info) //设置要读取hbase的表名

    //读取数据并转化成rdd
    val hBaseRDD = sc.newAPIHadoopRDD(
      hbaseConf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //将rdd装换为dataframe
    val order_info : DataFrame = hBaseRDD.map(r=>(
      Bytes.toString(r._2.getValue(Bytes.toBytes("MM"),Bytes.toBytes("driver_id"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("MM"),Bytes.toBytes("id"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("MM"),Bytes.toBytes("close_gps_time"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("MM"),Bytes.toBytes("create_time")))
    )).toDF("driver_id" , "id_" , "close_gps_time" , "create_time")

    //显示读取到的hbase表
//    order_info.show()

    order_info
  }

  //读取hbase数据库的driver_info表（司机信息表）
  def readHbase_driver_info(sparkConf : SparkConf , sparkSession : SparkSession): DataFrame = {
    val sc = sparkSession.sparkContext
    //###########用于使用  toDF   ###############
    val sqlContext = SparkSession.builder().config(sparkConf).getOrCreate().sqlContext
    import sqlContext.implicits._
    //###########################################
    //初始化HBaseConfiguration
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", GlobalConfigUtils.hbaseQuorem)    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    hbaseConf.set("hbase.zookeeper.property.clientPort", GlobalConfigUtils.clientPort)  //设置zookeeper连接端口，默认2181
    hbaseConf.set(TableInputFormat.INPUT_TABLE, GlobalConfigUtils.table_driver_info) //设置要读取hbase的表名

    //读取数据并转化成rdd
    val hBaseRDD = sc.newAPIHadoopRDD(
      hbaseConf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //将rdd装换为dataframe
    val driver_info : DataFrame = hBaseRDD.map(r=>(
      Bytes.toString(r._2.getValue(Bytes.toBytes("MM"),Bytes.toBytes("driver_type"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("MM"),Bytes.toBytes("id"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("MM"),Bytes.toBytes("mobile"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("MM"),Bytes.toBytes("driver_name"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("MM"),Bytes.toBytes("register_city"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("MM"),Bytes.toBytes("city_name"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("MM"),Bytes.toBytes("driver_management_id")))
    )).toDF("driver_type" , "id_" , "mobile" , "driver_name" , "register_city" , "city_name" , "driver_management_id" )

    //显示读取到的hbase表
//    driver_info.show(100)

    driver_info
  }

}
