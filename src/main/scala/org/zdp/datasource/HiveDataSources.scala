package org.zdp.datasource

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import org.zdp.entity.{DataSourceInfo, InputDataSourceInfo, OutputDataSourceInfo}

/**
  * 通过配置文件方式读写hive
  */
object HiveDataSources extends ZDPDataSources{

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def getSchema(spark: SparkSession, dataSourceInfo: DataSourceInfo)(implicit dispatch_task_id:String): Array[StructField] = {
    logger.info(s"获取hive表的schema信息table:${dataSourceInfo.dbTable},option:${dataSourceInfo.option.mkString(",")}")
    spark.table(dataSourceInfo.dbTable).schema.fields
  }

  override def getDS(spark: SparkSession, dispatchOption: Map[String, Any] = null,
                     inputDataSourceInfo: InputDataSourceInfo, duplicateCols:Array[String] = null,
                     selectColumn:Array[Column], sql: String = null)
                    (implicit dispatch_task_id: String = "-1"): DataFrame = {
    try {
      logger.info("[数据采集]:输入源为[HIVE]")
      val tableName = inputDataSourceInfo.dbTable
      if (tableName.trim.equals("")) {
        throw new Exception("[zdh],hive数据源读取:tableName为空")
      }
      logger.info("[数据采集]:[HIVE]:[READ]:[table]:"+tableName+"[FILTER]:"+inputDataSourceInfo.filter)
      val df = spark.table(tableName)
      filter(df,inputDataSourceInfo.filter,duplicateCols,selectColumn)
    } catch {
      case ex:Exception=>{
        logger.error("[数据采集]:[HIVE]:[READ]:[ERROR]:"+ex.getMessage.replace("\"","'"))
        throw ex
      }
    }
  }


  override def writeDS(sparkSession: SparkSession, df:DataFrame, outputDataSourceInfo: OutputDataSourceInfo)
                      (implicit dispatch_task_id:String = "-1"): Unit = {
    try {
      val options = outputDataSourceInfo.option
      logger.info("[数据采集]:[HIVE]:[WRITE]:[options]:"+options.mkString(","))
      //默认是append
      val model = options.getOrElse("model","").toString.toLowerCase match {
        case "overwrite" => SaveMode.Overwrite
        case "append" => SaveMode.Append
        case "errorifexists" => SaveMode.ErrorIfExists
        case "ignore" => SaveMode.Ignore
        case _ => SaveMode.Append
      }

      //如果需要建立外部表需要options中另外传入path 参数 example 外部表t1 path:/dir1/dir2/t1
      val format = options.getOrElse("format","orc")
      val tableName = outputDataSourceInfo.dbTable
      val partitionBy = options.getOrElse("partitionBy","")

      var df_tmp = df
      //合并小文件操作
      if(!options.getOrElse("merge","-1").equals("-1")){
        df_tmp = df.repartition(options.getOrElse("merge","200").toString.toInt)
      }
      val baseOption = DataSourceInfo.getBaseOption(outputDataSourceInfo)
      if (sparkSession.catalog.tableExists(tableName)) {
        val cols = sparkSession.table(tableName).columns
        df_tmp.select(cols.map(col):_*)
          .write
          .mode(model)
//          .options(baseOption)
          .insertInto(tableName)
      } else {
        // saveAsTable 可以创建一个新表，这里暂时不使用
//        if (partitionBy.equals("")) {
//          df_tmp.write
//            .mode(model)
//            .format(format)
////            .options(options)
////            .options(baseOption)
//            .saveAsTable(tableName)
//        } else {
//          df_tmp.write.mode(model).format(format)
//            .partitionBy(partitionBy)
////            .options(options)
////            .options(baseOption)
//            .saveAsTable(tableName)
//        }
        logger.error("[数据采集]:[HIVE]:[WRITE]:[ERROR]: 未找到该表")
      }
    } catch {
      case ex:Exception=>{
        ex.printStackTrace()
        logger.error("[数据采集]:[HIVE]:[WRITE]:[ERROR]:"+ex.getMessage.replace("\"","'"))
        throw ex
      }
    }
  }

}
