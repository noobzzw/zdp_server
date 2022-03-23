package org.zdp.entity


// 数据源类型，对应数据库中的DataSourceInfo
class DataSourceInfo {
  var id:String = _
  var dataSourceContent:String = _
  var dataSourceType:String = _
  var driver:String = _
  var url:String = _
  var username:String = _
  var password:String = _
  var dbTable:String = _
  var option:Map[String, Any] = _

  def this(param:Map[String,Any], option:Map[String,Any] = Map.empty) {
    // scala 第一行必须调用默认构造器
    this()
    this.id  = param.getOrElse("id","").toString
    this.dataSourceContent = param.getOrElse("data_source_context","").toString
    this.dataSourceType = param.getOrElse("data_source_type", "").toString
    this.driver = param.getOrElse("driver","").toString
    this.url = param.getOrElse("url","").toString
    this.username = param.getOrElse("user","").toString
    this.password = param.getOrElse("password","").toString
    val dbtable = param.getOrElse("dbtable","").toString
    val paths = param.getOrElse("paths","").toString
    if (dbtable.equals("")) {
      this.dbTable = paths
    } else {
      this.dbTable = dbtable
    }
    this.option = param ++ option
  }
}

object DataSourceInfo {
  /**
   * 获取基本的连接信息
   */
  def getBaseOption(dataSourceInfo: DataSourceInfo): Map[String,String] = {
    Map("url" -> dataSourceInfo.url,
      "dbtable" -> dataSourceInfo.dbTable,
      "user" -> dataSourceInfo.username,
      "password" -> dataSourceInfo.password,
      "driver" -> dataSourceInfo.driver)
  }
}

class InputDataSourceInfo(param:Map[String,Any], option:Map[String,Any] = Map.empty, var cols:Array[String], var filter:String) extends DataSourceInfo(param,option)

class OutputDataSourceInfo(param:Map[String,Any], option:Map[String,Any] = Map.empty, var clear:String, var columns:Array[Map[String,String]]) extends DataSourceInfo(param,option)
