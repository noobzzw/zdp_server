package org.zdp.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkBuilder{
  private var sparkSession:SparkSession = _
  private def initSparkSession(): Unit ={
    val sparkConf = new SparkConf()
    val system = System.getProperty("os.name").toLowerCase();
    // 本地模式
    if(system.startsWith("win") || system.startsWith("mac")){
      sparkConf.setMaster("local[*]")
    }
    sparkConf.setAppName("ZZW Data Platform Spark Server")
    // 序列化方式
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("hive.orc.splits.include.file.footer","true")
    sparkConf.set("hive.exec.orc.default.stripe.size","268435456")
    sparkConf.set("hive.exec.orc.split.strategy", "BI")
    sparkConf.set("spark.sql.orc.impl","hive")
    sparkConf.set("spark.sql.hive.convertMetastoreOrc","false")
    sparkConf.set("spark.sql.orc.enableVectorizedReader","false")
    // 开启cross join
    sparkConf.set("spark.sql.crossJoin.enabled","true")
    sparkConf.set("spark.extraListeners", classOf[ServerSparkListener].getName)
//    sparkConf.set("spark.sql.shuffle.partitions","2000")
//    sparkConf.set("spark.sql.extensions","org.apache.spark.sql.TiExtensions")
//    sparkConf.set("spark.tispark.pd.addresses","192.168.110.10:2379")
    val sparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .config("spark.sql.warehouse.dir","hdfs://localhost:8020/data/hive/warehouse")
      .config("hive.metastore.uris","thrift://0.0.0.0:9083")
      .config("hive.exec.scratchdir", "hdfs://localhost:8020/data/hive/tmp")
      .enableHiveSupport()
      .getOrCreate()
    // 谓词下推
    sparkSession.sql("set spark.sql.orc.filterPushdown=true")
    sparkSession.sparkContext.setLogLevel("WARN")
    this.sparkSession = sparkSession
  }


  /**
   * 单例SparkSession
   * @return
   */
  def getSparkSession: SparkSession ={
    synchronized{
      if(sparkSession == null){
        initSparkSession()
      }
    }
    sparkSession
  }
}
