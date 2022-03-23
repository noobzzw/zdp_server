package org.zdp.entity;

import com.alibaba.fastjson.JSON
import org.junit.Assert._
import org.junit.Test;

class EtlTaskInfoTest {

  @Test
  def testEtlTaskInfoJson(): Unit = {
    val etlJson = """
                    |{
                    |        "column_data_list":[
                    |            {
                    |                "column_alias":"id",
                    |                "column_expr":"id",
                    |                "column_md5":"603-4267-9a26-f1",
                    |                "column_name":"id",
                    |                "column_type":"string"
                    |            }
                    |        ],
                    |        "column_datas":"[{\"column_md5\":\"603-4267-9a26-f1\",\"column_name\":\"id\",\"column_expr\":\"id\",\"column_type\":\"string\",\"column_alias\":\"id\"}]",
                    |        "column_size":"",
                    |        "company":"",
                    |        "create_time":1642582006000,
                    |        "data_source_type_input":"JDBC",
                    |        "data_source_type_output":"JDBC",
                    |        "data_sources_choose_input":"67",
                    |        "data_sources_choose_output":"67",
                    |        "data_sources_clear_output":"",
                    |        "data_sources_file_columns":"",
                    |        "data_sources_file_name_input":"",
                    |        "data_sources_file_name_output":"",
                    |        "data_sources_filter_input":"",
                    |        "data_sources_params_input":"",
                    |        "data_sources_params_output":"",
                    |        "data_sources_table_columns":"id",
                    |        "data_sources_table_name_input":"t_test",
                    |        "data_sources_table_name_output":"t_test",
                    |        "duplicate_columns":"",
                    |        "encoding_input":"",
                    |        "encoding_output":"",
                    |        "error_rate":"",
                    |        "etl_context":"mysql2mysql",
                    |        "file_type_input":"",
                    |        "file_type_output":"",
                    |        "header_input":"",
                    |        "header_output":"",
                    |        "id":"933402112996413440",
                    |        "owner":"1",
                    |        "primary_columns":"",
                    |        "rows_range":"",
                    |        "section":"",
                    |        "sep_input":"",
                    |        "sep_output":"",
                    |        "service":"",
                    |        "update_context":""
                    |}""".stripMargin
    val columnData = """{
                       |  "column_alias":"id",
                       |  "column_expr":"id",
                       |  "column_md5":"603-4267-9a26-f1",
                       |  "column_name":"id",
                       |  "column_type":"string"
                       |}""".stripMargin
    val jsonObject = JSON.parseObject(etlJson,classOf[EtlTaskInfo])
    val columnDataObj = JSON.parseObject(columnData, classOf[ColumnData])
    println(jsonObject.toString)
    println(columnDataObj)
    assertEquals(1642582006000L,jsonObject.getCreateTime.getTime)
    assertEquals("603-4267-9a26-f1",columnDataObj.getColumnMd5)
  }


}
