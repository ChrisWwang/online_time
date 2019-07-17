package dongfeng.query.hbase

import java.io.Serializable
import java.util

import dongfeng.code.tools.spark.GlobalConfigUtils
import org.apache.hadoop.hbase.client.{Row => _, _}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
/**
  * Created by angel
  */
object Resolver extends  Serializable {
  def resolve (hbaseField: HBaseSchemaField, result: Result ): Any = {
    val cfColArray = hbaseField.fieldName.split(":",-1)
    val cfName = cfColArray(0)
    val colName =  cfColArray(1)
    var fieldRs: Any = null
    //resolve row key otherwise resolve column
    if(cfName=="" && colName=="key") {
      fieldRs = resolveRowKey(result, hbaseField.fieldType)
    } else {
      fieldRs =  resolveColumn(result, cfName, colName,hbaseField.fieldType)
    }
    fieldRs
  }

  def resolveRowKey (result: Result, resultType: String): Any = {
    val rowkey = resultType match {
      case "String" =>
        result.getRow.map(_.toChar).mkString
      case "Int" =>
        result  .getRow.map(_.toChar).mkString.toInt
      case "Long" =>
        result.getRow.map(_.toChar).mkString.toLong
      case "Float" =>
        result.getRow.map(_.toChar).mkString.toLong
      case "Double" =>
        result.getRow.map(_.toChar).mkString.toDouble
    }
    rowkey
  }

  def resolveColumn (result: Result, columnFamily: String, columnName: String, resultType: String): Any = {

    val column = result.containsColumn(columnFamily.getBytes, columnName.getBytes) match{
      case true =>{
        resultType match {
          case "String" =>
            Bytes.toString(result.getValue(columnFamily.getBytes,columnName.getBytes))
          case "Int" =>
            Bytes.toInt(result.getValue(columnFamily.getBytes,columnName.getBytes))
          case "Long" =>
            Bytes.toLong(result.getValue(columnFamily.getBytes,columnName.getBytes))
          case "Float" =>
            Bytes.toFloat(result.getValue(columnFamily.getBytes,columnName.getBytes))
          case "Double" =>
            Bytes.toDouble(result.getValue(columnFamily.getBytes,columnName.getBytes))

        }
      }
      case _ => {
        resultType match {
          case "String" =>
            ""
          case "Int" =>
            0
          case "Long" =>
            0
          case "Double" =>
            0.0
        }
      }
    }
    column
  }
}


case class HBaseRelation(@transient val hbaseProps: Map[String,String])(@transient val sqlContext: SQLContext) extends BaseRelation with Serializable with TableScan{

  val hbaseTableName =  hbaseProps.getOrElse("hbase_table_name", sys.error("not valid schema"))
  val hbaseTableSchema =  hbaseProps.getOrElse("hbase_table_schema", sys.error("not valid schema"))
  val registerTableSchema = hbaseProps.getOrElse("sparksql_table_schema", sys.error("not valid schema"))
//  val begin_time = hbaseProps.getOrElse("begin_time", sys.error("not valid schema"))
//  val end_time = hbaseProps.getOrElse("end_time", sys.error("not valid schema"))
  val rowRange = hbaseProps.getOrElse("row_range", "->")
  //get star row and end row
  val range = rowRange.split("->",-1)
  val startRowKey = range(0).trim
  val endRowKey = range(1).trim
  val tempHBaseFields = extractHBaseSchema(hbaseTableSchema) //do not use this, a temp field
  val registerTableFields = extractRegisterSchema(registerTableSchema)
  val tempFieldRelation = tableSchemaFieldMapping(tempHBaseFields,registerTableFields)
  val hbaseTableFields = feedTypes(tempFieldRelation)
  val fieldsRelations =  tableSchemaFieldMapping(hbaseTableFields,registerTableFields)
  val queryColumns =  getQueryTargetCloumns(hbaseTableFields)

  def feedTypes( mapping: util.LinkedHashMap[HBaseSchemaField, RegisteredSchemaField]) :  Array[HBaseSchemaField] = {
    val hbaseFields = mapping.map{
      case (k,v) =>
        val field = k.copy(fieldType=v.fieldType)
        field
    }
    hbaseFields.toArray
  }





  def isRowKey(field: HBaseSchemaField) : Boolean = {
    val cfColArray = field.fieldName.split(":",-1)
    val cfName = cfColArray(0)
    val colName =  cfColArray(1)
    if(cfName=="" && colName=="key") true else false
  }

  //eg: f1:col1  f1:col2  f1:col3  f2:col1
  def getQueryTargetCloumns(hbaseTableFields: Array[HBaseSchemaField]): String = {
    var str = ArrayBuffer[String]()
    hbaseTableFields.foreach{ field=>
      if(!isRowKey(field)) {
//        str +=  field.fieldName
        str.append(field.fieldName)
      }
    }
    str.mkString(" ")
//    str.toString()
  }
  lazy val schema = {
    val fields = hbaseTableFields.map{ field=>
      val name  = fieldsRelations.getOrElse(field, sys.error("table schema is not match the definition.")).fieldName
      val relatedType =  field.fieldType match  {
        case "String" =>
          SchemaType(StringType,nullable = false)
        case "Int" =>
          SchemaType(IntegerType,nullable = false)
        case "Long" =>
          SchemaType(LongType,nullable = false)
        case "Double" =>
          SchemaType(DoubleType,nullable = false)

      }
      StructField(name,relatedType.dataType,relatedType.nullable)
    }
    StructType(fields)
  }
  def tableSchemaFieldMapping( externalHBaseTable: Array[HBaseSchemaField],  registerTable : Array[RegisteredSchemaField]): util.LinkedHashMap[HBaseSchemaField, RegisteredSchemaField] = {
    if(externalHBaseTable.length != registerTable.length) sys.error("columns size not match in definition!")
    val rs: Array[(HBaseSchemaField, RegisteredSchemaField)] = externalHBaseTable.zip(registerTable)
    val linkedHashMap = new util.LinkedHashMap[HBaseSchemaField, RegisteredSchemaField]()
    for(arr <- rs){
      linkedHashMap.put(arr._1 , arr._2)
    }
    linkedHashMap
  }


  /**
    * spark dfsql schema will be register
    *   registerTableSchema   '(rowkey string, value string, column_a string)'
    */
  def extractRegisterSchema(registerTableSchema: String) : Array[RegisteredSchemaField] = {
    val fieldsStr = registerTableSchema.trim.drop(1).dropRight(1)
    val fieldsArray = fieldsStr.split(",").map(_.trim)//sorted
    fieldsArray.map{ fildString =>
      val splitedField = fildString.split("\\s+", -1)//sorted
      RegisteredSchemaField(splitedField(0), splitedField(1))
    }
  }

  //externalTableSchema '(:key , f1:col1 )'
  def extractHBaseSchema(externalTableSchema: String) : Array[HBaseSchemaField] = {
    val fieldsStr = externalTableSchema.trim.drop(1).dropRight(1)
    val fieldsArray = fieldsStr.split(",").map(_.trim)
    fieldsArray.map(fildString => HBaseSchemaField(fildString,""))
  }

  // By making this a lazy val we keep the RDD around, amortizing the cost of locating splits.
  lazy val buildScan = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", GlobalConfigUtils.hbaseQuorem)
    hbaseConf.set(TableInputFormat.INPUT_TABLE, hbaseTableName)
    hbaseConf.set(TableInputFormat.SCAN_COLUMNS, queryColumns)
    hbaseConf.set(TableInputFormat.SCAN_ROW_START, startRowKey)
    hbaseConf.set(TableInputFormat.SCAN_ROW_STOP, endRowKey)
//    hbaseConf.set(TableInputFormat.SCAN_BATCHSIZE , "2")//TODO 此处导致查询数据不一致
    hbaseConf.set(TableInputFormat.SCAN_CACHEDROWS , "10000")
    hbaseConf.set(TableInputFormat.SHUFFLE_MAPS , "1000")



    //添加时间的过滤
    val tableName = hbaseTableName
    var connection: Connection = null
    val scan: Scan = new Scan
//    scan.setStartRow(Bytes.toBytes("0000"+begin_time+"|"))
//    scan.setStopRow(Bytes.toBytes("0007"+end_time+"|"))
    scan.setCacheBlocks(false)
    var table: Table = null
    connection = ConnectionFactory.createConnection(hbaseConf);
    table = connection.getTable(TableName.valueOf(tableName));
    //TODO 尽量不要使用filter，如果非要使用，应该配合startrwo endrow
//    val filterList = new FilterList();
//    if (begin_time != null) {
//      val rf = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new RegexStringComparator(".*"+begin_time));
//      filterList.addFilter(rf);
//    }
//    if (end_time != null) {
//      val rf = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new RegexStringComparator(".*"+begin_time));
//      filterList.addFilter(rf);
//    }
//    scan.setFilter(filterList);
    hbaseConf.set(TableInputFormat.SCAN , convertScanToString(scan))
    val hbaseRdd = sqlContext.sparkContext.newAPIHadoopRDD(
      hbaseConf,
      classOf[org.apache.hadoop.hbase.mapreduce.TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )

    val rs: RDD[Row] = hbaseRdd.map(tuple => tuple._2).map(result => {

      var values = new ArrayBuffer[Any]()
      hbaseTableFields.foreach { field =>
        values += Resolver.resolve(field, result)
      }
      Row.fromSeq(values.toSeq)
    })
    rs
  }
  def convertScanToString(scan: Scan):String={
    val proto = ProtobufUtil.toScan(scan)
    return Base64.encodeBytes(proto.toByteArray)
  }
  private case class SchemaType(dataType: DataType, nullable: Boolean)


}
