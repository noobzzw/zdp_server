package org.zdp.etl

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, length, lit, udf}
import org.zdp.entity.ColumnData

// todo 重写质量检测
object QualityHandler {
  val column_is_null = "column_is_null"
  val column_length = "column_length"
  val column_regex = "column_regex"
  val column_name = "column_name"

  /**
   * 检测表的元数据（字段）是否符合要求
   * @param outputCols 每个字段以及检测要求
   */
//  def metaDataDetection(outputCols: List[ColumnData]): Map[String,Seq[Column]] = {
//    // 列值是否为null检测
//    var columnIsNull: Seq[Column] = Seq.empty[Column]
//    var columnLength: Seq[Column] = Seq.empty[Column]
//    var columnRegex: Seq[Column] = Seq.empty[Column]
//    // 创建一个用于正则表达书的udf
//    val regexUdf = udf((c1:String,re:String)=>{
//      val a = re.r()
//      c1.matches(a.toString())
//    })
//
//    outputCols.foreach(f => {
//      if (f.getOrElse(column_is_null, "true").trim.equals("false")) {
//        if (f.getOrElse(column_name, "").trim.equals("")) {
//          throw new Exception("字段是否为空检测,需要的原始字段列名不为空")
//        }
//        val c1 = col(f.getOrElse(column_name, "").trim).isNull
//        columnIsNull = columnIsNull.:+(c1)
//      }
//      if (!f.getOrElse(column_regex, "").trim.equals("")) {
//        if (f.getOrElse(column_name, "").trim.equals("")) {
//          throw new Exception("字段表达式检测,需要的原始字段列名不为空")
//        }
//        val c1 = regexUdf(col(f.getOrElse(column_name, "").trim),lit(f.getOrElse(column_regex, "").trim))
//        columnRegex = columnRegex.:+(c1)
//      }
//      if (!f.getOrElse(column_length, "").trim.equals("")) {
//        if (f.getOrElse("column_name", "").trim.equals("")) {
//          throw new Exception("字段长度质量检测,需要的原始字段列名不为空")
//        }
//        val c1 = length(col(f.getOrElse(column_name, "").trim)) =!= f.getOrElse(column_length, "").trim
//        columnLength = columnLength.:+(c1)
//      }
//    })
//    Map(column_is_null -> columnIsNull, column_length ->columnLength, column_regex -> columnRegex)
//  }

}
