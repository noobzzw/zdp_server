package org.zdp
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory
import org.zdp.netty.NettyServer
object SystemStart {
  private val logger = LoggerFactory.getLogger(SystemStart.getClass)
  private val configLoader = ConfigFactory.load("application.conf")

  def main(args: Array[String]): Unit = {
    val serverConf = configLoader.getConfig("server")
    val host:String = if (serverConf.getString("host").equals("localhost")) {
      "127.0.0.1"
    } else {
      Some[String](serverConf.getString("host")).getOrElse("127.0.0.1")
    }
    val port = Some[Int](serverConf.getInt("port")).getOrElse(60002)
    logger.info(s"启动netty http服务器线程: ${host}:${port}")
    /* 启动netty http服务器 */
    new NettyServer().start(host,port)
  }

}
