package org.zdp.datasource

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import org.zdp.entity.{DataSourceInfo, InputDataSourceInfo, OutputDataSourceInfo}

import java.util

trait ZDPDataSources {

  private val logger = LoggerFactory.getLogger(this.getClass)
  /**
    * 获取schema
    *
    * @param spark
    * @return
    */
  def getSchema(spark: SparkSession, dataSourceInfo: DataSourceInfo)(implicit dispatch_task_id: String = "-1"): Array[StructField]

  /**
   * 将DataSource转为DataFrame
   * @param spark sparkSession
   * @param dispatchOption 分发任务时的map
   * @param inputDataSourceInfo 输入数据源
   * @param duplicateCols 需要去除重复的列
   * @param selectColumn 选中的输出列
   * @param sql 进行条件处理的sql
   * @param dispatch_task_id dispatch_task_id
   * @return
   */
  def getDS(spark: SparkSession, dispatchOption: Map[String, Any],
            inputDataSourceInfo: InputDataSourceInfo, duplicateCols:Array[String],
            selectColumn:Array[Column],
            sql: String)(implicit dispatch_task_id: String): DataFrame


  /**
   * 过滤操作(包含where和distinct)
   * @param df 要过滤的df
   * @param filterCondition 关系表达式
   * @param duplicateCols 去重的列
   * @param select 选择的列
   * @return 修改后的df
   */
  def filter(df:DataFrame, filterCondition: String, duplicateCols:Array[String],select: Array[Column]): DataFrame ={
    if (filterCondition!=null && !filterCondition.trim.equals("")) {
      df.filter(filterCondition)
    } else  if(duplicateCols!=null && duplicateCols.length>0){
      df.dropDuplicates(duplicateCols)
    } else {
      df
    }
    try {
      logger.info("[数据采集]:[SELECT]")
      logger.debug("[数据采集]:[SELECT]:"+select.mkString(","))
      if(select==null || select.isEmpty){
        logger.debug("[数据采集]:[SELECT]:[智能识别字段]" +df.columns.mkString(","))
        return df
      }
      // _* 是吧参数转换为变长序列（当入参为变长参数时，传入一个集合需要做如此改动）
      df.select(select: _*)
    } catch {
      case ex:Exception=>{
        logger.error("[数据采集]:[SELECT]:[ERROR]"+ex.getMessage.replace("\"","'"))
        throw ex
      }
    }
  }



  /**
    * 写入数据总入口
    * @param options 参数
    * @param sql 写入数据的sql
    * @param dispatch_task_id 当前任务id
    */
  def writeDS(sparkSession: SparkSession, df: DataFrame, outputDataSourceInfo: OutputDataSourceInfo)(implicit dispatch_task_id: String): Unit


  /**
   * 质量校验
   * @param error_rate 限定的错误率
   * @param primary_columns 主键
   * @param column_size 限定的列数
   * @param rows_range 限定的行的范围
   * @param column_is_null 非空列
   * @param column_length
   * @param column_regex
   * @return
   */
  def dataQuality(spark: SparkSession, df: DataFrame, error_rate: String,
                  primary_columns: String, column_size: Int, rows_range: String,
                  column_is_null: Seq[Column], column_length: Seq[Column],
                  column_regex: Seq[Column]): Map[String, String] = {
    val report = new util.HashMap[String, String]()
    report.put("result", "通过")
    import scala.collection.JavaConverters._
    try {
      import spark.implicits._
      val total_count = df.count()
      report.put("总行数", total_count.toString)
      var error_count = 0L;
      //表级质量检测
      //判断主键是否重复
      if (!primary_columns.trim.equals("")) {
        val cols: Array[Column] = primary_columns.split(",").map(col)
        val new_count = df.select(concat_ws("_", cols: _*) as "primary_key") // 将主键拼在一起
          .map(f => (f.getAs[String]("primary_key"), 1)) //转化为(k,1)
          .rdd.reduceByKey(_ + _) //reduce
        val primary_keys = new_count.filter(f => f._2 > 1)
        val primary_count = primary_keys.count()
        if (primary_count > 0) {
          error_count = error_count + primary_count
          logger.info("存在主键重复")
          report.put("primary_columns", "存在主键重复")
          report.put("result", "不通过")
        } else {
          logger.info("主键检测通过")
          report.put("primary_columns", "主键检测通过")
        }
      }

      //判断解析的列数是否一致
      if (column_size > 0) {
        if (df.columns.length != column_size) {
          report.put("column_size", "解析字段个数不对")
          logger.info("解析字段个数不对")
          report.put("result", "不通过")
        } else {
          report.put("column_size", "字段个数检测通过")
          logger.info("字段个数检测通过")
        }
      }

      //判断行数
      if (!rows_range.equals("")) {
        var start_count = 0
        var end_count = 0
        if(rows_range.contains("-")){
          start_count = rows_range.split("-")(0).toInt
          end_count = rows_range.split("-")(1).toInt
        }else{
          start_count = rows_range.toInt
          end_count = start_count
        }

        if (total_count < start_count || total_count > end_count) {
          logger.info("数据行数异常")
          report.put("rows_range", "数据行数异常")
          report.put("result", "不通过")
        } else {
          report.put("rows_range", "数据行数检测通过")
          logger.info("数据行数检测通过")
        }
      }

      //字段级检测
      //是否为空检测,
      //col("").isNull
      if (column_is_null.size > 0) {
        val filter_columns = column_is_null.tail.foldLeft(column_is_null.head)((x, y) => {
          x or y
        })

        val null_count = df.filter(filter_columns).count()
        if (null_count > 0) {
          error_count = error_count + null_count
          report.put("column_is_null", "存在字段为空,但是此字段不允许为空")
          report.put("result", "不通过")
          logger.info("存在字段为空,但是此字段不允许为空")
        } else {
          report.put("column_is_null", "字段是否为空,检测通过")
          logger.info("字段是否为空,检测通过")
        }
      }


      //
      //length(col(""))==长度
      if (column_length.nonEmpty) {
        val filter_column_length = column_length.tail.foldLeft(column_length.head)((x, y) => {
          x or y
        })

        val length_count = df.filter(filter_column_length).count()
        if (length_count > 0) {
          error_count = error_count + length_count
          report.put("column_length", "存在字段长度不满足")
          report.put("result", "不通过")
          logger.info("存在字段长度不满足")
        } else {
          logger.info("字段长度检测通过")
          report.put("column_length", "字段长度检测通过")
        }
      }

      if (column_regex.nonEmpty) {

        val c_r=column_regex.map(f=>f==="false")
        val filter_column_regex = c_r.tail.foldLeft(c_r.head)((x, y) => {
          x or y
        })
        val regex_count = df.filter(filter_column_regex).count()
        if (regex_count > 0) {
          error_count = error_count + regex_count
          report.put("column_regex", "正则判断不通过")
          report.put("result", "不通过")
          logger.info("存在正则不满足")
        } else {
          logger.info("正则判断检测通过")
          report.put("column_regex", "正则判断检测通过")
        }
      }

      logger.info("error_count:" + error_count)

      // 容错检查
      var error_num = total_count * error_rate.toDouble
      if (error_num < 1) {
        logger.info("容错的条数至少是1条")
        error_num = 1
      }
      if (error_count <= error_num && error_count!=0) {
        logger.info("在指定容错率范围内")
        report.put("result", "容错率内")
      }
      report.asScala.toMap[String, String]
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }


  }
}
