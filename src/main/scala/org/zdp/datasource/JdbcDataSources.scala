package org.zdp.datasource

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import org.zdp.entity.{DataSourceInfo, InputDataSourceInfo, OutputDataSourceInfo}

import java.util.Properties

/**
  * 使用此数据源连接所有的jdbc数据,包括hive,mysql,oracle 等
  */
object JdbcDataSources extends ZDPDataSources{

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * 获取数据源schema
    *
    * @param spark
    * @param options
    * @return
    */
  override def getSchema(spark: SparkSession, dataSourceInfo: DataSourceInfo)(implicit dispatch_task_id:String): Array[StructField] = {
    val options = DataSourceInfo.getBaseOption(dataSourceInfo)
    logger.info("[数据采集]:[JDBC]:[SCHEMA]:"+options.mkString(","))
    spark.read.format("jdbc").options(options).load().schema.fields
  }


  override def getDS(spark: SparkSession, dispatchOption: Map[String, Any] = null,
                     inputDataSourceInfo: InputDataSourceInfo, duplicateCols:Array[String] = null,
                     selectColumn:Array[Column],sql: String = null)(implicit dispatch_task_id: String): DataFrame = {
    try{
      logger.info("[数据采集]:输入源为[JDBC],开始匹配对应参数")
      checkBaseParams(inputDataSourceInfo)
      logger.info("[数据采集]:[JDBC]:[READ]:表名:" + inputDataSourceInfo.dbTable + "," + inputDataSourceInfo.option.mkString(",") + " [FILTER]:" + inputDataSourceInfo.filter)
      //获取jdbc 配置
      val url = inputDataSourceInfo.url
      val dbtable = inputDataSourceInfo.dbTable
      var format = "jdbc"
      // 暂时只支持hive和ck
      if (url.contains("jdbc:hive2:")) {
        format = "org.apache.spark.sql.execution.datasources.hive.HiveRelationProvider"
        logger.info("[数据采集]:[JDBC]:[READ]:表名:"+dbtable+",使用自定义hive-jdbc数据源")
      }
      else if (url.contains("jdbc:clickhouse:")) {
        format = "org.apache.spark.sql.execution.datasources.clickhouse.ClickHouseRelationProvider"
        logger.info("[数据采集]:[JDBC]:[READ]:表名:"+dbtable+",使用自定义clickhouse-jdbc数据源")
      }
      // 加载jdbc datasource
      val df: DataFrame = spark.read
        .format(format)
//        .options(inputDataSourceInfo.option.asInstanceOf[Map[String,String]])
        .options(DataSourceInfo.getBaseOption(inputDataSourceInfo))
        .load()
      filter(df,inputDataSourceInfo.filter,duplicateCols,selectColumn)
    } catch {
      case ex:Exception=>{
        logger.error("[数据采集]:[JDBC]:[READ]:表名:"+inputDataSourceInfo.dbTable+"[ERROR]:"+ex.getMessage.replace("\"","'"),"error")
        throw ex
      }
    }

  }


  /**
   * JDBC连接时检测基本参数
   */
  private def checkBaseParams(inputDataSourceInfo: InputDataSourceInfo): Unit = {
    val baseOptions = DataSourceInfo.getBaseOption(inputDataSourceInfo)
    for (key <- baseOptions.keys) {
      if (baseOptions(key).trim.equals("")) {
        throw new Exception(s"[ZDP],jdbc数据源读取: ${key}为空!")
      }
    }
  }

  override def writeDS(sparkSession: SparkSession, df:DataFrame, outputDataSourceInfo: OutputDataSourceInfo)(implicit dispatch_task_id:String): Unit = {
    val options = outputDataSourceInfo.option.asInstanceOf[Map[String,String]]
    try{
      logger.info("[数据采集]:[JDBC]:[WRITE]:表名:"+options.getOrElse("dbtable","")+","+options.mkString(","))
      val dbtable: String = outputDataSourceInfo.dbTable
      if(dbtable.trim.equals("")){
        throw new Exception("[数据采集]:[JDBC]:[WRITE]:dbtable参数为空")
      }
      val url = outputDataSourceInfo.url
      // 暂时去除预先处理表的功能
//      if (!sql.equals("")) {
//        deleteJDBC(sparkSession,url,options,sql)
//      }
      var format="jdbc"
      if(url.toLowerCase.contains("jdbc:hive2:")){
        format="org.apache.spark.sql.hive_jdbc.datasources.hive.HiveRelationProvider"
        logger.info("[数据采集]:[JDBC]:[WRITE]:表名:"+options.getOrElse("dbtable","")+",使用自定义hive-jdbc数据源")
      }
      if(url.toLowerCase.contains("jdbc:clickhouse:")){
        format="org.apache.spark.sql.hive_jdbc.datasources.clickhouse.ClickHouseRelationProvider"
        logger.info("[数据采集]:[JDBC]:[WRITE]:表名:"+options.getOrElse("dbtable","")+",使用自定义clickhouse-jdbc数据源")
      }
      df.write
        .format(format)
        .mode(SaveMode.Append)
//        .options(options) // 暂时取消其他option，会导致空指针，估计是json哪个字段导致的
        .options(DataSourceInfo.getBaseOption(outputDataSourceInfo))
        .save()
    } catch {
      case ex:Exception=>{
        ex.printStackTrace()
        logger.info("[数据采集]:[JDBC]:[WRITE]:表名:"+options.getOrElse("dbtable","")+","+"[ERROR]:"+ex.getMessage.replace("\"","'"))
        throw ex
      }
    }

  }


  /**
    * 写入jdbc 之前 清空特定数据
    *
    * @param spark
    * @param url
    * @param options
    * @param sql
    */
  @Deprecated
  def deleteJDBC(spark: SparkSession, url: String, options:  Map[String,String], sql: String)(implicit dispatch_task_id:String): Unit = {
    logger.info("[数据采集]:[JDBC]:[CLEAR]:url:"+url+","+options.mkString(",")+",sql:"+sql)
    val properties = new Properties()
    options.keys.foreach((key:String) => properties.put(key,options(key)))
//    properties.putAll(options.asJava)
    var driver = properties.getProperty("driver", "")
    if (driver.equals("")) {
      driver = getDriver(url)
    }
    Class.forName(driver)
    var cn: java.sql.Connection = null
    var ps: java.sql.PreparedStatement = null
    try {
      cn = java.sql.DriverManager.getConnection(url, properties)
      ps = cn.prepareStatement(sql)
      ps.execute()
      ps.close()
      cn.close()
    }
    catch {
      case ex: Exception => {
        ps.close()
        cn.close()
        if(ex.getMessage.replace("\"","'").contains("doesn't exist") || ex.getMessage.replace("\"","'").contains("Unknown table")){
          logger.warn("[数据采集]:[JDBC]:[CLEAR]:[WARN]:"+ex.getMessage.replace("\"","'"))
        }else{
          throw ex
        }
      }
    }
  }


  def getDriver(url: String): String = {
    url match {
      case u if u.toLowerCase.contains("jdbc:mysql") => "com.mysql.jdbc.Driver"
      case _ => ""
    }
  }

}
