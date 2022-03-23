package org.zdp.entity


/**
 * "column_data_list":[
            {
                "column_alias":"id",
                "column_expr":"id",
                "column_md5":"603-4267-9a26-f1",
                "column_name":"id",
                "column_type":"string"
            }
        ],
        "column_datas":"[{\"column_md5\":\"603-4267-9a26-f1\",\"column_name\":\"id\",\"column_expr\":\"id\",\"column_type\":\"string\",\"column_alias\":\"id\"}]",
        "column_size":"",
        "company":"",
        "create_time":1642582006000,
        "data_source_type_input":"JDBC",
        "data_source_type_output":"JDBC",
        "data_sources_choose_input":"67",
        "data_sources_choose_output":"67",
        "data_sources_clear_output":"",
        "data_sources_file_columns":"",
        "data_sources_file_name_input":"",
        "data_sources_file_name_output":"",
        "data_sources_filter_input":"",
        "data_sources_params_input":"",
        "data_sources_params_output":"",
        "data_sources_table_columns":"id",
        "data_sources_table_name_input":"t_test",
        "data_sources_table_name_output":"t_test",
        "duplicate_columns":"",
        "encoding_input":"",
        "encoding_output":"",
        "error_rate":"",
        "etl_context":"mysql2mysql",
        "file_type_input":"",
        "file_type_output":"",
        "header_input":"",
        "header_output":"",
        "id":"933402112996413440",
        "owner":"1",
        "primary_columns":"",
        "rows_range":"",
        "section":"",
        "sep_input":"",
        "sep_output":"",
        "service":"",
        "update_context":""
 */
class EtlTaskInfo2 {
  var id: String = _
  var etl_context: String = _
  //输入数据源id
  var data_sources_choose_input: String = _
  //输入数据源类型
  var data_source_type_input: String = _
  //输入数据源表名
  var data_sources_table_name_input: String = _
  //输入数据源文件名
  var data_sources_file_name_input: String = _
  //输入数据源文件中字段名
  var data_sources_file_columns: String = _
  //输入数据源表字段名
  var data_sources_table_columns: String = _
  //文件类型
  var file_type_input: String = _
  //文件编码
  var encoding_input: String = _
  //是否有头标题
  var header_input: String = _
  //文件分割符
  var sep_input: String = _
  //输入数据源其他参数
  var data_sources_params_input: String = _

  //输入数据源过滤条件
  var data_sources_filter_input: String = _

  //输出数据源id
  var data_sources_choose_output: String = _
  //输出数据源类型
  var data_source_type_output: String = _
  //输出数据源表名
  var data_sources_table_name_output: String = _
  //输出数据源文件名
  var data_sources_file_name_output: String = _
  var file_type_output: String = _
  var encoding_output: String = _
  var header_output: String = _
  var sep_output: String = _
  //输出数据源其他参数
  var data_sources_params_output: String = _

  //输入-输出 字段映射关系json
  var column_datas: String = _
  //输出数据源删除条件
  var data_sources_clear_output: String = _

  //输入-输出 字段映射关系class
  var column_data_list: List[ColumnData] = _

  var owner: String = _

  var create_time: Int = _

  var company: String = _

  var section: String = _

  var service: String = _

  var update_context: String = _

  var primary_columns: String = _

  var column_size: String = _

  var rows_range: String = _ //xxx-xxx


  var error_rate: String = _ //容错率


//  var enable_quality: String = _

  var duplicate_columns: String = _ //去重字段


  override def toString = s"EtlTaskInfo($id, $etl_context, $data_sources_choose_input, $data_source_type_input, $data_sources_table_name_input, $data_sources_file_name_input, $data_sources_file_columns, $data_sources_table_columns, $file_type_input, $encoding_input, $header_input, $sep_input, $data_sources_params_input, $data_sources_filter_input, $data_sources_choose_output, $data_source_type_output, $data_sources_table_name_output, $data_sources_file_name_output, $file_type_output, $encoding_output, $header_output, $sep_output, $data_sources_params_output, $column_datas, $data_sources_clear_output, $column_data_list, $owner, $create_time, $company, $section, $service, $update_context, $primary_columns, $column_size, $rows_range, $error_rate, $duplicate_columns)"
}
