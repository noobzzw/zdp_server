package org.zdp.dao

import java.sql.Timestamp


trait TaskLogInstanceMapper {

  def update(taskLogId:String, jobId:String
             , status:String, etlDate:String
             ,process:String): Int

  def updateTime(taskLogId:String, jobId:String
                 , status:String, etlDate:String,
                 retryTime:Timestamp): Int
  def updateProcess(taskLogId:String,process:String)
}
