package org.zdp.dao

import org.junit.Test
import org.zdp.util.MybatisUtil
import org.junit.Assert._
class TaskLogInstanceMapperTest {
  @Test
  def testDB: Unit = {
    val taskLogInstanceMapper = MybatisUtil.getSqlSession.getMapper(classOf[TaskLogInstanceMapper])
    val taskLogInstanceId = "944166709105790976"
    val jobId = "933402746747359232"
    val etlDate = "2022-02-18 09:41:22"
    val status = "error"
    val process = "100"
    val result = taskLogInstanceMapper.update(taskLogInstanceId, jobId, status, etlDate, process)
    assertEquals(result,1)
  }
}
