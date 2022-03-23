package org.zdp.util

import org.apache.ibatis.io.Resources
import org.apache.ibatis.session.SqlSession
import org.apache.ibatis.session.SqlSessionFactory
import org.apache.ibatis.session.SqlSessionFactoryBuilder

import java.io.IOException

object MybatisUtil {
    private var sqlSessionFactory: SqlSessionFactory = _

    try {
        val resource = "mybatis-config.xml";
        // util无需关闭流
        val  inputStream = Resources.getResourceAsStream(resource);
        sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
    } catch {
        case ex:IOException => ex.printStackTrace();
    }

    /**
     * 通过factory openSession
     * @return
     */
    def getSqlSession: SqlSession = {
        // 自动提交事务
        sqlSessionFactory.openSession(true)
    }

}
