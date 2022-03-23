package org.zdp.entity;

import lombok.Data;

public class ColumnData {
  private String columnMd5;
  private String columnName;
  private String columnExpr;
  private String columnType;
  private String columnLength;
  private String columnIsNull;
  private String columnRegex;
  private String columnDesc;
  private String columnAlias;

  public String getColumnMd5() {
    return columnMd5;
  }

  public void setColumnMd5(String columnMd5) {
    this.columnMd5 = columnMd5;
  }

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public String getColumnExpr() {
    return columnExpr;
  }

  public void setColumnExpr(String columnExpr) {
    this.columnExpr = columnExpr;
  }

  public String getColumnType() {
    if ("".equals(columnType)) {
      return "string";
    }
    return columnType;
  }

  public void setColumnType(String columnType) {
    this.columnType = columnType;
  }

  public String getColumnLength() {
    return columnLength;
  }

  public void setColumnLength(String columnLength) {
    this.columnLength = columnLength;
  }

  public String getColumnIsNull() {
    return columnIsNull;
  }

  public void setColumnIsNull(String columnIsNull) {
    this.columnIsNull = columnIsNull;
  }

  public String getColumnRegex() {
    return columnRegex;
  }

  public void setColumnRegex(String columnRegex) {
    this.columnRegex = columnRegex;
  }

  public String getColumnDesc() {
    return columnDesc;
  }

  public void setColumnDesc(String columnDesc) {
    this.columnDesc = columnDesc;
  }

  public String getColumnAlias() {
    return columnAlias;
  }

  public void setColumnAlias(String columnAlias) {
    this.columnAlias = columnAlias;
  }
}
