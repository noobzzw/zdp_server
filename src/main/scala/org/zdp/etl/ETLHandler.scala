package org.zdp.etl

import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.{LoggerFactory, MDC}
import org.zdp.dao.TaskLogInstanceMapper
import org.zdp.datasource.{HiveDataSources, JdbcDataSources, ZDPDataSources}
import org.zdp.entity.{DataSourceInfo, InputDataSourceInfo, OutputDataSourceInfo}
import org.zdp.spark.ServerSparkListener
import org.zdp.util.{DateUtil, JsonUtil, MybatisUtil}

import java.sql.Timestamp
import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import java.util.{Calendar, Date}

object ETLHandler {
  private val logger = LoggerFactory.getLogger(this.getClass)
  // 线程池，用于异步执行
  private val pool = new ThreadPoolExecutor(
    3, // core pool size
    10, // max pool size
    500, // keep alive time
    TimeUnit.MILLISECONDS, // unit of time
    new LinkedBlockingQueue[Runnable]() // "链表组成的队列，最大为Integer.MAX_VALUE"
  )
  // spark 处理时的标志
  val SPARK_ZDP_PROCESS = "spark.zdp.process"
  /* mybatis sql session and mapper
      sql session 线程不安全，里面只有一个connect
      但是一般仅影响读和写，这里只有写的场景
   */
  private val sqlSession = MybatisUtil.getSqlSession
  private val taskLogInstanceMapper = sqlSession.getMapper(classOf[TaskLogInstanceMapper])


  /**
   * 进行ELT任务
   * @param param etl param
   * @return http response
   */
  def executeETLTask(param: Map[String, Any],sparkSession: SparkSession): Boolean = {
    /* task info */
    // 任务task_logs_id
    val task_logs_id = param.getOrElse("task_logs_id", "001").toString
    // etl任务信息
    val etlTaskInfo = param.getOrElse("etlTaskInfo", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    // 调度任务信息
    val dispatchOptions = param.getOrElse("tli", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    /* datasource input info */
    //输入数据源信息
    val dsi_Input: Map[String, Any] = param.getOrElse("dsi_Input", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    //输入数据源其他信息k:v,k1:v1 格式
    val inputOptions: Map[String, Any] = etlTaskInfo.getOrElse("data_sources_params_input", "").toString.trim match {
      case "" => Map.empty[String, Any]
      case a => JsonUtil.jsonToMap(a)
    }
    // datasource input columns
    val inputCols: Array[String] = etlTaskInfo.getOrElse("data_sources_file_columns", "").toString.split(",")
    //过滤条件
    val filter = etlTaskInfo.getOrElse("data_sources_filter_input", "").toString
    // 生成对象
    val inputDataSourceInfo = new InputDataSourceInfo(dsi_Input, inputOptions, inputCols, filter)

    /* datasource output */
    //输出数据源信息
    val dsi_Output = param.getOrElse("dsi_Output", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    //输出数据源其他信息k:v,k1:v1 格式
    val outputOptions: Map[String, Any] = etlTaskInfo.getOrElse("data_sources_params_output", "").toString match {
      case "" => Map.empty[String, Any]
      case a => JsonUtil.jsonToMap(a)
    }
    //字段映射
    val listMap = Option(etlTaskInfo.getOrElse("column_data_list", null).asInstanceOf[List[Map[String, String]]]).getOrElse(List[Map[String,String]]())
    // 输出字段
    val outPutCols = listMap.toArray
    //清空语句
    val clear = etlTaskInfo.getOrElse("data_sources_clear_output", "").toString
    // 生成对象
    val outputDataSourceInfo = new OutputDataSourceInfo(dsi_Output,outputOptions,clear,outPutCols)
    pool.execute(new Runnable() {
      override def run() = {
        try {
          dataHandler(sparkSession, task_logs_id, dispatchOptions, etlTaskInfo, inputDataSourceInfo, outputDataSourceInfo)
        } catch {
          case ex: Exception => {
            ex.printStackTrace()
          }
        }
      }
    })
    true
  }



  /**
   * 统一数据源处理入口
   *
   * @param sparkSession     sparkSession
   * @param taskLogsId     任务记录id(数据库自增id)
   * @param dispatchOption   调度任务相关参数
   */
  private def dataHandler(sparkSession: SparkSession, taskLogsId: String, dispatchOption: Map[String, Any], etlTaskInfo: Map[String, Any],
                          inputDataSourceInfo: InputDataSourceInfo, outputDataSourceInfo: OutputDataSourceInfo): Unit = {
    // job_id为当前任务id，由后端传入，task_log_id为数据库自增id
    implicit val jobId: String = dispatchOption.getOrElse("job_id", "001").toString
    val etlDate = JsonUtil.jsonToMap(dispatchOption.getOrElse("params", "").toString).getOrElse("ETL_DATE", "").toString
    val jobContext = dispatchOption.getOrElse("jobContext", "001").toString
    MDC.put("job_id", jobId)
    MDC.put("task_logs_id",taskLogsId)
    // 更新进度
    taskLogInstanceMapper.update(taskLogsId,jobId,"etl",etlDate,"23")
    val sparkSessionCurrent = sparkSession.newSession()
    /*
        为当前任务创建一个jobGroup，用于标识与后续任务取消
        jobGroupId 为 任务记录id_job_context
     */
    val sparkJobGroupID =  taskLogsId+"_"+jobContext
    val sparkJobGroupDescription = etlTaskInfo.getOrElse("etl_context",etlTaskInfo.getOrElse("id","").toString).toString+"_"+etlDate+"_"+taskLogsId
    sparkSessionCurrent.sparkContext
      .setJobGroup(sparkJobGroupID,sparkJobGroupDescription)
    try {
      logger.info(s"[数据采集]:数据采集开始，日期:${etlDate}")
      /* 获取输入数据 */
      // 输出文件类型
      val outputFileType = etlTaskInfo.getOrElse("file_type_output","csv").toString
      // 输出文件编码
      val outputEncoding = etlTaskInfo.getOrElse("encoding_output","utf-8").toString
      val header = etlTaskInfo.getOrElse("header_output","false").toString
      val sep = etlTaskInfo.getOrElse("sep_output",",").toString
      val primary_columns = etlTaskInfo.getOrElse("primary_columns", "").toString
      outputDataSourceInfo.option = outputDataSourceInfo.option.asInstanceOf[Map[String,String]]
        .+("outputFileType" -> outputFileType, "outputEncoding" -> outputEncoding,
          "sep" -> sep,"header" -> header)
      //加载Spark配置 参数 spark. 开头的都是conf 配置
      inputDataSourceInfo.option.filter(p => p._1.startsWith("spark."))
        .asInstanceOf[Map[String,String]]
        .foreach(p => sparkSessionCurrent.conf.set(p._1,p._2))
      // 设置当前任务为input任务
      sparkSessionCurrent.conf.set(ServerSparkListener.SPARK_ZDP_PROCESS,"INPUT")

      // 获取输入数据源
      val inputDF = inPutHandler(sparkSessionCurrent, taskLogsId, dispatchOption, etlTaskInfo,
        inputDataSourceInfo,outputDataSourceInfo)

      // todo 流式数据ETL，预计采用Flink

      /* 数据输出 */
      val inputType = inputDataSourceInfo.dataSourceType
      if (!inputType.toLowerCase.equals("kafka") && !inputType.toLowerCase.equals("flume")) {
        // 设置spark参数
        outputDataSourceInfo.option.filter(p=>p._1.startsWith("spark."))
          .asInstanceOf[Map[String,String]]
          .foreach(p=>sparkSessionCurrent.conf.set(p._1,p._2))
        // 设置当前为output任务
        sparkSessionCurrent.conf.set(ServerSparkListener.SPARK_ZDP_PROCESS,"OUTPUT")
        // 处理输出
        outPutHandler(sparkSessionCurrent, inputDF, outputDataSourceInfo)
        // 更新任务进度
        taskLogInstanceMapper.update(taskLogsId,jobId,"finish",etlDate,"100")
      } else {
        logger.info("[数据采集]:数据采集检测是实时采集,程序将常驻运行")
      }
      logger.info("[数据采集]:数据采集完成")
    } catch {
        case ex: Exception => {
          ex.printStackTrace()
          logger.error("[数据采集]:[ERROR]:" + ex.getMessage, ex.getCause)
          // 错误处理
          errorHandler(taskLogsId, jobId, etlDate, dispatchOption)
        }
    } finally {
      MDC.remove("job_id")
      MDC.remove("task_logs_id")
      SparkSession.clearActiveSession()
    }
  }

  /**
   * 读取数据源handler
   * @param sparkSession spark session
   * @param task_logs_id 当前task运行id
   * @param dispatchOption 分发任务时的option（包括date等参数）
   * @param etlTaskInfo etl任务等参数（包括编码等）
   * @param inPut 输入数据源
   * @param inputOptions 输入数据源参数
   * @param inputCondition 过滤条件
   * @param inputCols 输入数据源列
   * @param outPut 输出数据源
   * @param outputOptions 输出数据源参数
   * @param outputCols 输出数据源列
   * @param sql 过滤sql
   * @param dispatch_task_id dispatch_task_id
   * @return DataFrame封装的输入数据源
   */
  private def inPutHandler(sparkSession: SparkSession, task_logs_id: String, dispatchOption: Map[String, Any], etlTaskInfo: Map[String, Any],
                           inputDataSourceInfo: InputDataSourceInfo, outputDataSourceInfo: OutputDataSourceInfo)(implicit dispatch_task_id: String): DataFrame = {
    //调用对应的数据源
    //调用对应的中间处理层
    logger.info("[数据采集]:[输入]:开始匹配输入数据源")
    // elt 日期
    val etlDate = JsonUtil.jsonToMap(dispatchOption.getOrElse("params", "").toString).getOrElse("ETL_DATE", "").toString
    // 容错率率
    val errorRate = etlTaskInfo.getOrElse("errorRate", "0.01").toString match {
      case "" => "0.01"
      case er => er
    }

    // 重复的列
    val duplicateColumns = etlTaskInfo.getOrElse("duplicateColumns", "").toString.trim match {
      case "" => Array.empty[String]
      case a => a.split(",")
    }
    val fileType = etlTaskInfo.getOrElse("file_type_input","csv").toString
    val encoding = etlTaskInfo.getOrElse("encoding_input","utf-8").toString
    val header = etlTaskInfo.getOrElse("header_input","false").toString
    val sep = etlTaskInfo.getOrElse("sep_input",",").toString
    inputDataSourceInfo.option =  inputDataSourceInfo.option
      .asInstanceOf[Map[String,String]]
      .+("fileType"->fileType,"encoding"->encoding,"sep"->sep,"header"->header)
    // 匹配数据源
    val inputType = inputDataSourceInfo.dataSourceType
    val zdpDataSources: ZDPDataSources = inputType.toLowerCase match {
      case "jdbc" => JdbcDataSources
      case "hive" => HiveDataSources
      case _ => throw new Exception("数据源类型无法匹配")
    }
    var outputColsExpr: Array[Column] = null
    if (!inputType.toLowerCase.equals("hbase")) {
      outputColsExpr = outputDataSourceInfo.columns
        .map(column => outputColumnMap(column,etlDate))
    }
    if (outputColsExpr==null) {
      outputColsExpr = Array.empty[Column]
    }

    val primaryColumns = etlTaskInfo.getOrElse("primary_columns", "").toString
    val columnSize = etlTaskInfo.getOrElse("column_size", "").toString match {
      case "" => 0
      case cs => cs.toInt
    }
    val rowsRange = etlTaskInfo.getOrElse("rows_range", "").toString
    logger.info("完成加载ETL任务转换信息")

    // 更新task处理进度
    taskLogInstanceMapper.update(task_logs_id,dispatch_task_id,"etl",etlDate,"25")
    // 这里需要用output的column，因为原作者抽象不好，把这个输出归到了output里
    val result = zdpDataSources.getDS(sparkSession, dispatchOption, inputDataSourceInfo,
      duplicateColumns, outputColsExpr, inputDataSourceInfo.filter)
    taskLogInstanceMapper.update(task_logs_id,dispatch_task_id,"etl",etlDate,"61")
    // 开启质量检验
    val enableQuality = etlTaskInfo.getOrElse("enableQuality", "off").toString
    if (enableQuality.trim.equals("on") && !inputDataSourceInfo.dataSourceType.equalsIgnoreCase("kafka")) {
      logger.info("任务开启了质量检测,开始进行质量检测")
      logger.info("开始加载ETL任务转换信息,检测元数据是否合规")
      val outputColsResult = QualityHandler.metaDataDetection(outputDataSourceInfo.columns)
      val report = zdpDataSources.dataQuality(sparkSession, result, errorRate, primaryColumns, columnSize, rowsRange,
        outputColsResult(QualityHandler.column_name),
        outputColsResult(QualityHandler.column_length),
        outputColsResult(QualityHandler.column_regex))
      if (report.getOrElse("result", "").equals("不通过")) {
        throw new Exception("ETL 任务做质量检测时不通过,具体请查看质量检测报告")
      }
      logger.info("完成质量检测")
    } else {
      logger.info("未开启质量检测,如果想开启,请打开ETL任务中质量检测开关,提示:如果输入数据源是kafka 不支持质量检测")
    }

    // 分区
    val repartitionNum = inputDataSourceInfo.option.getOrElse("repartition_num","").toString
    val repartitionCols = inputDataSourceInfo.option.getOrElse("repartition_cols","").toString
    if(!repartitionNum.equals("") && !repartitionCols.equals("")){
      // 重分区个数与字段均不为空
      logger.info("数据重分区规则,重分区个数:"+repartitionNum+",重分区字段:"+repartitionCols)
      return result.repartition(repartitionNum.toInt,repartitionCols.split(",").map(col):_*)
    } else if (repartitionNum.equals("") && !repartitionCols.equals("")) {
      // 重分区数量为空，字段不为空
      logger.info("数据重分区规则,重分区字段:"+repartitionCols+",无分区个数")
      // same operation as "DISTRIBUTE BY"
      return result.repartition(repartitionCols.split(",").map(col):_*)
    } else if (!repartitionNum.equals("")&& repartitionCols.equals("")) {
      // 重分区数量不为空，无分区字段
      logger.info("数据重分区规则,重分区个数:"+repartitionNum+",无分区字段")
      return result.repartition(repartitionNum.toInt)
    }
    result
  }

  /**
   * 对输出数据进行处理
   * @param spark sparkSession
   * @param df 要处理的dataFrame
   * @param outPut 输出源标识
   * @param outputOptionions 输出源参数
   * @param sql 写入数据前，清空原有数据的sql
   * @param jobId 调度任务id
   */
  def outPutHandler(spark: SparkSession, df: DataFrame,
                    outputDataSourceInfo: OutputDataSourceInfo)
                   (implicit jobId: String): Unit = {
    try {
      logger.info("[数据采集]:[输出]:开始匹配输出数据源")
      //调用写入数据源
      val zdpDataSources: ZDPDataSources = outputDataSourceInfo.dataSourceType.toLowerCase match {
        case "jdbc" => {
          logger.info("[数据采集]:[输出]:输出源为[JDBC]")
          JdbcDataSources
        }
        case "hive" => {
          logger.info("[数据采集]:[输出]:输出源为[HIVE]")
          HiveDataSources
        }
        case x => throw new Exception("[数据采集]:[输出]:无法识别输出数据源:" + x)
      }
      zdpDataSources.writeDS(spark, df,outputDataSourceInfo)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.error("[数据采集]:[输出]:[ERROR]:" + ex.getMessage)
        throw ex
      }
    }
  }

  /**
   * 对输出列进行处理，包括运行表达式，列别名等
   * @param column
   * @param etlDate
   * @return
   */
  def outputColumnMap(column: Map[String, String], etlDate:String): Column = {
    if (column.getOrElse("column_alias", "").toLowerCase.equals("row_key")) {
      expr(column.getOrElse("column_expr", ""))
        .cast("string")
        .as(column.getOrElse("column_alias", ""))
    } else {
      // 如果类型参数不为空，则进行类型转换
      if (!column.getOrElse("column_type", "").trim.equals("")) {
        // 如果存在时间参数，则进行替换
        if (column.getOrElse("column_expr", "").contains("$zdh_etl_date")) {
          expr(column.getOrElse("column_expr", "")
            .replaceAll("\\$zdh_etl_date", "'" + etlDate + "'"))
            .cast(column.getOrElse("column_type", "string"))
            .as(column.getOrElse("column_alias", ""))
        } else {
          expr(column.getOrElse("column_expr", ""))
            .cast(column.getOrElse("column_type", "string"))
            .as(column.getOrElse("column_alias", ""))
        }
      } else {
        // 默认类型
        if (column.getOrElse("column_expr", "").contains("$zdh_etl_date")) {
          expr(column.getOrElse("column_expr", "")
            .replaceAll("\\$zdh_etl_date", "'" + etlDate + "'"))
            .as(column.getOrElse("column_alias", ""))
        } else {
          expr(column.getOrElse("column_expr", ""))
            .as(column.getOrElse("column_alias", ""))
        }
      }
    }
  }


  /**
   * 错误处理封装方法
   */
  def errorHandler(task_logs_id: String, jobId: String, etlDate: String,dispatchOption:Map[String,Any]): Unit = {
    val retryNum = dispatchOption.getOrElse("count","1").toString.toInt
    val planRetryNum = dispatchOption.getOrElse("plan_count","3").toString.toInt
    val intervalTime = if(dispatchOption.getOrElse("interval_time","").toString.equalsIgnoreCase("")) 5 else dispatchOption.getOrElse("interval_time","5").toString.toInt
    // 错误处理
    errorHandler0(task_logs_id,jobId,etlDate,retryNum,planRetryNum,intervalTime)
  }

  /**
   * 错误处理
   * @param task_logs_id 当前task id(数据库自增id)
   * @param jobId 当前job_id(人为指定)
   * @param etlDate eltDate
   * @param retryNum 当前重试的次数
   * @param planRetryNum 计划重试的次数
   * @param intervalTime 重试任务的间隔
   */
  def errorHandler0(task_logs_id: String, jobId: String, etlDate: String, retryNum:Int, planRetryNum:Int, intervalTime:Int): Unit = {
    var status = "error"
    var msg = "ETL任务失败存在问题,重试次数已达到最大,状态设置为error"
    if (planRetryNum == -1 && retryNum < planRetryNum) {
      status = "wait_retry"
      msg = "ETL任务失败存在问题,状态设置为wait_retry等待重试"
      if (planRetryNum == -1) msg = msg + ",并检测到重试次数为无限次"
    }
    logger.info(msg)
    val retryTime = DateUtil.add(new Timestamp(new Date().getTime),Calendar.SECOND,intervalTime)
    if (status.equals("wait_retry")) {
      // 更新进度
      taskLogInstanceMapper.updateTime(task_logs_id,jobId,status,etlDate,retryTime)
    } else {
      // 更新status和process
      taskLogInstanceMapper.update(task_logs_id,jobId,status,etlDate,"")
    }
  }




}
