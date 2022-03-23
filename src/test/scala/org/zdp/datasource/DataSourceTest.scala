package org.zdp.datasource

import org.apache.log4j.{Level, Logger}
import org.junit._
import org.zdp.entity.InputDataSourceInfo
import org.zdp.spark.SparkBuilder
class DataSourceTest {
  /**
   * 测试hive连接
   */
  @Test
  def testHive(): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val sparkSession = SparkBuilder.getSparkSession
    sparkSession.sql("select * from test.student").show()
    sparkSession.sql("show databases").show()
    val option = Map("table" -> "test.student","paths" -> "test.student","tableName" -> "test.student")
//    HiveDataSources.getSchema(sparkSession,option)("1").foreach(println)
//    val studentDataFrame = HiveDataSources.getDS(sparkSession, inputOptions = option, inputCondition = null)
//    val sql = """insert into student values(2,"wyj")"""
//    HiveDataSources.writeDS(sparkSession, studentDataFrame, option, sql)
  }

  @Test
  def testJdbc(): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val sparkSession = SparkBuilder.getSparkSession
//    val option = Map("url" -> "jdbc:mysql://127.0.0.1:3306?serverTimezone=GMT%2B8&useSSL=false&allowPublicKeyRetrieval=true",
//      "user" -> "root", "password" -> "zzw0105wyj",
//      "dbtable" -> "test.t_test","driver" -> "com.mysql.cj.jdbc.Driver")
//    JdbcDataSources.getSchema(sparkSession,option).foreach(println)
//    JdbcDataSources.getDS(sparkSession)

  }

}
