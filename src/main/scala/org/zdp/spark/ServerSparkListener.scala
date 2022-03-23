package org.zdp.spark

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerTaskEnd, StageInfo}
import org.slf4j.LoggerFactory
import org.zdp.dao.TaskLogInstanceMapper
import org.zdp.spark.ServerSparkListener.{SPARK_ZDP_PROCESS, taskLogInstanceMapper}
import org.zdp.util.MybatisUtil


object ServerSparkListener{
  // sparkListener 监听的标志
  val SPARK_ZDP_PROCESS = "spark.zdp.process"
  //存储job id 和进度
  val jobs = new java.util.concurrent.ConcurrentHashMap[Int,Int]
  //job->stage
  val stages = new java.util.concurrent.ConcurrentHashMap[Int,Seq[StageInfo]]
  //job->tasknum
  val tasks = new java.util.concurrent.ConcurrentHashMap[Int,Int]
  // stage->job
  val stage_job = new java.util.concurrent.ConcurrentHashMap[Int,Int]
  // job -> task_log_instance
  val job_tli = new java.util.concurrent.ConcurrentHashMap[Int,String]
  // mapper
  private val sqlSession = MybatisUtil.getSqlSession
  private val taskLogInstanceMapper = sqlSession.getMapper(classOf[TaskLogInstanceMapper])
}

/**
 * 自定义SparkListener，用于进度管理
 */
class ServerSparkListener extends SparkListener{
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    // 输出配置
//    jobStart.properties.keySet()
//      .toArray
//      .foreach(key=> println(key+"==="+jobStart.properties.getProperty(key.toString)))
    // 仅仅监控有SPARK_ZDP_PROCESS标识的
    val PROCESS = jobStart.properties.getProperty(SPARK_ZDP_PROCESS)
    println("Process:"+PROCESS)
    val processNum:Int = PROCESS match {
      case "INPUT" => 25
      case "OUTPUT" => 61
    }
    //获取对应的task_log_instance_id
    val taskLogInstanceId = jobStart.properties.getProperty("spark.jobGroup.id").split("_")(0)
    ServerSparkListener.job_tli.put(jobStart.jobId,taskLogInstanceId)
    taskLogInstanceMapper.updateProcess(taskLogInstanceId,processNum.toString)
    ServerSparkListener.jobs.put(jobStart.jobId,processNum)
    val totalTasks = jobStart.stageInfos.map(stage => stage.numTasks).sum
    ServerSparkListener.stages.put(jobStart.jobId,jobStart.stageInfos)
    ServerSparkListener.tasks.put(jobStart.jobId,totalTasks)
    jobStart.stageIds.map(sid=> ServerSparkListener.stage_job.put(sid,jobStart.jobId))
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    // 及时清空数据
    ServerSparkListener.jobs.remove(jobEnd.jobId)
    ServerSparkListener.stages.get(jobEnd.jobId).foreach(stage=>{
      ServerSparkListener.stage_job.remove(stage.stageId)
    })
    ServerSparkListener.tasks.remove(jobEnd.jobId)
    ServerSparkListener.job_tli.remove(jobEnd.jobId)
    ServerSparkListener.stages.remove(jobEnd.jobId)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    println("Stage id:" + taskEnd.stageId)
  }


  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageId=stageCompleted.stageInfo.stageId
    //stage 获取job
    val jobId = ServerSparkListener.stage_job.get(stageId)
    //获取所有的任务数
    val totalTasks = ServerSparkListener.tasks.get(jobId)
    //获取job_id对应的task_log_instance id
    val taskLogInstanceId = ServerSparkListener.job_tli.get(jobId)
    val newProcessNum = ServerSparkListener.jobs.get(jobId) + 35*(stageCompleted.stageInfo.numTasks/totalTasks)
    ServerSparkListener.jobs.put(jobId,newProcessNum)
    taskLogInstanceMapper.updateProcess(taskLogInstanceId,newProcessNum.toString)
  }

}
