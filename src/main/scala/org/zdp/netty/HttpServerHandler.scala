package org.zdp.netty

import io.netty.channel.{ChannelFutureListener, ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.codec.http._
import io.netty.util.CharsetUtil
import org.slf4j.{LoggerFactory, MDC}
import org.zdp.dao.TaskLogInstanceMapper
import org.zdp.etl.ETLHandler
import org.zdp.spark.{SparkBuilder, SparkHandler}
import org.zdp.util.{JsonUtil, MybatisUtil}

import java.net.URLDecoder
import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}


/**
 * 处理Http请求的handler
 */
class HttpServerHandler extends ChannelInboundHandlerAdapter with HttpBaseHandler {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val spark = SparkBuilder.getSparkSession
  //单线程线程池，同一时间只会有一个线程在运行,保证加载顺序
  private val Deadpool = new ThreadPoolExecutor(
    1, // core pool size
    1, // max pool size
    500, // keep alive time
    TimeUnit.MILLISECONDS, // unit of time
    new LinkedBlockingQueue[Runnable]() // "链表组成的队列，最大为Integer.MAX_VALUE"
  )
  private val taskLogInstanceMapper: TaskLogInstanceMapper = MybatisUtil.getSqlSession.getMapper(classOf[TaskLogInstanceMapper])

  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
    logger.info("Spark(netty)端接受到http消息")
    val request = msg.asInstanceOf[FullHttpRequest]
    val keepAlive = HttpUtil.isKeepAlive(request)
    // 分发请求并处理
    val response = dispatcher(request)
    if (keepAlive) {
      response.headers().set(Connection, KeepAlive)
      ctx.writeAndFlush(response)
    } else {
      ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE)
    }
  }

  /**
    * 分发请求
    *
    * @param request 后端发来的请求
    * @return response
    */
  def dispatcher(request: FullHttpRequest): HttpResponse = {
    val uri = request.uri()
    //解析param
    val param = getReqContent(request)
    val dispatchOptions = param.getOrElse("tli", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    val jobId = dispatchOptions.getOrElse("job_id", "001").toString
    val taskLogsId=param.getOrElse("task_logs_id", "001").toString
    // jsonToMap 无法处理""，增加判断逻辑
    var etlDate: String = ""
    if (dispatchOptions.contains("params")) {
      etlDate = JsonUtil.jsonToMap(dispatchOptions.getOrElse("params", "").toString).getOrElse("ETL_DATE", "").toString
    }
    try {
      // MDC 为线程专用的日志，为kv类型的
      MDC.put("job_id", jobId)
      MDC.put("task_logs_id",taskLogsId)
      taskLogInstanceMapper.update(taskLogsId,jobId,"etl",etlDate,"22")
      logger.info(s"接收到请求uri:$uri,参数:${param.mkString(",").replaceAll("\"", "")}")
      if (uri.contains(killUri)) {
        killJobs(param)
      } else if (uri.contains(keepAliveUri)) {
        defaultResponse(cmdOk)
      } else if (uri.contains(etlUri)) {
        val result = ETLHandler.executeETLTask(param,spark)
        if (result) defaultResponse(cmdOk) else defaultResponse(noUri)
      } else {
        defaultResponse(noUri)
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        // error handle
        ETLHandler.errorHandler(taskLogsId,jobId,etlDate,dispatchOptions)
        defaultResponse(noUri)
      }
    } finally {
      MDC.remove("job_id")
      MDC.remove("task_logs_id")
    }

  }

  /**
   * spark作业组批量取消
   * @param param  传入参数
   * @return
   */
  private def killJobs(param: Map[String,Any]): DefaultFullHttpResponse = {
    val task_logs_id = param.getOrElse("task_logs_id", "001").toString
    val dispatch_task_id = param.getOrElse("job_id", "001").toString
    val jobGroupList = param.getOrElse("jobGroups",List.empty[String]).asInstanceOf[List[String]]
    // mdc 为轻量的日志，这里将日志写入zdh_log这个表里
    MDC.put("job_id", dispatch_task_id)
    MDC.put("task_logs_id",task_logs_id)
    val result = SparkHandler.kill(jobGroupList)
    MDC.remove("job_id")
    MDC.remove("task_logs_id")
    if (result) {
      defaultResponse(cmdOk)
    } else {
      defaultResponse(execErr)
    }
  }

  private def getBody(content: String): Map[String, Any] = {
    JsonUtil.jsonToMap(content)
  }

  private def getParam(uri: String): Map[String, Any] = {
    val path = URLDecoder.decode(uri, chartSet)
    val cont = uri.substring(path.lastIndexOf("?") + 1)
    if (cont.contains("="))
      cont.split("&").map(f => (f.split("=")(0), f.split("=")(1))).toMap[String, Any]
    else
      Map.empty[String, Any]
  }

  private def getReqContent(request: FullHttpRequest): Map[String, Any] = {
    request.method() match {
      case HttpMethod.GET => getParam(request.uri())
      case HttpMethod.POST => getBody(request.content.toString(CharsetUtil.UTF_8))
    }
  }




}
