package org.zdp.spark

import org.slf4j.{LoggerFactory, MDC}

/**
 * Spark作业处理类
 */
class SparkHandler {}
object SparkHandler {
  private val logger = LoggerFactory.getLogger(classOf[SparkHandler])
  // spark session
  private val sparkSession = SparkBuilder.getSparkSession

  /**
   * 取消选定的作业
   * @param jobGroupList 要取消的作业集
   * @return 是否取消成功
   */
  def kill(jobGroupList: List[String]): Boolean = {
    // 启动spark job
    logger.info(s"开始杀死任务: ${jobGroupList.mkString(",")}")
    if (jobGroupList.isEmpty) {
      logger.warn("JogGroups is null!!")
      false
    } else {
      jobGroupList.foreach(jobGroup=>{
        sparkSession.sparkContext.cancelJobGroup(jobGroup)
        logger.info(s"杀死任务:$jobGroup")
      })
      logger.info(s"完成杀死任务:${jobGroupList.mkString(",")}")
      true
    }
  }
}
