package org.zdp.entity;

import java.sql.Timestamp;
import java.util.List;

/**
 * EtlTaskInfo，对应json字段：etlTaskInfo
 * 因为采用fastJson将对象从json中抽取出来，所以使用Java Pojo而非Scala的class
 */
public class EtlTaskInfo {
    private String id;
    private String etlContext;
    //输入数据源id
    private String dataSourcesChooseInput;
    //输入数据源类型
    private String dataSourceTypeInput;
    //输入数据源表名
    private String dataSourcesTableNameInput;
    //输入数据源文件名
    private String dataSourcesFileNameInput;
    //输入数据源文件中字段名
    private String dataSourcesFileColumns;
    //输入数据源表字段名
    private String dataSourcesTableColumns;
    //文件类型
    private String fileTypeInput;
    //文件编码
    private String encodingInput;
    //是否有头标题
    private String headerInput;
    //文件分割符
    private String sepInput;
    //输入数据源其他参数
    private String dataSourcesParamsInput;

    //输入数据源过滤条件
    private String dataSourcesFilterInput;

    //输出数据源id
    private String dataSourcesChooseOutput;
    //输出数据源类型
    private String dataSourceTypeOutput;
    //输出数据源表名
    private String dataSourcesTableNameOutput;
    //输出数据源文件名
    private String dataSourcesFileNameOutput;
    //文件类型
    private String fileTypeOutput;
    //文件编码
    private String encodingOutput;
    //是否有头标题
    private Boolean headerOutput;
    //文件分割符
    private String sepOutput;
    //输出数据源其他参数
    private String dataSourcesParamsOutput;

    //输入-输出 字段映射关系json
    private String columnDatas;
    //输出数据源删除条件
    private String dataSourcesClearOutput;

    //输入-输出 字段映射关系class
    private List<ColumnData> columnDataList;

    private String owner;

    private Timestamp createTime;

    private String company;

    private String section;

    private String service;

    private String updateContext;

    private String primaryColumns;

    private String columnSize;

    private String rowsRange;//xxx-xxx

    private String errorRate;//容错率

    private String enableQuality;

    private String duplicateColumns;//去重字段

    private String merge;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getEtlContext() {
        return etlContext;
    }

    public void setEtlContext(String etlContext) {
        this.etlContext = etlContext;
    }

    public String getDataSourcesChooseInput() {
        return dataSourcesChooseInput;
    }

    public void setDataSourcesChooseInput(String dataSourcesChooseInput) {
        this.dataSourcesChooseInput = dataSourcesChooseInput;
    }

    public String getDataSourceTypeInput() {
        return dataSourceTypeInput;
    }

    public void setDataSourceTypeInput(String dataSourceTypeInput) {
        this.dataSourceTypeInput = dataSourceTypeInput;
    }

    public String getDataSourcesTableNameInput() {
        return dataSourcesTableNameInput;
    }

    public void setDataSourcesTableNameInput(String dataSourcesTableNameInput) {
        this.dataSourcesTableNameInput = dataSourcesTableNameInput;
    }

    public String getDataSourcesFileNameInput() {
        return dataSourcesFileNameInput;
    }

    public void setDataSourcesFileNameInput(String dataSourcesFileNameInput) {
        this.dataSourcesFileNameInput = dataSourcesFileNameInput;
    }

    public String getDataSourcesFileColumns() {
        return dataSourcesFileColumns;
    }

    public void setDataSourcesFileColumns(String dataSourcesFileColumns) {
        this.dataSourcesFileColumns = dataSourcesFileColumns;
    }

    public String getDataSourcesTableColumns() {
        return dataSourcesTableColumns;
    }

    public void setDataSourcesTableColumns(String dataSourcesTableColumns) {
        this.dataSourcesTableColumns = dataSourcesTableColumns;
    }

    public String getFileTypeInput() {
        if ("".equals(fileTypeInput)) {
            return "csv";
        }
        return fileTypeInput;
    }

    public void setFileTypeInput(String fileTypeInput) {
        this.fileTypeInput = fileTypeInput;
    }

    public String getEncodingInput() {
        if ("".equals(encodingInput)) {
            return "utf-8";
        }
        return encodingInput;
    }

    public void setEncodingInput(String encodingInput) {
        this.encodingInput = encodingInput;
    }

    public String getHeaderInput() {
        return headerInput;
    }

    public void setHeaderInput(String headerInput) {
        this.headerInput = headerInput;
    }

    public String getSepInput() {
        if ("".equals(sepInput)) {
            return ",";
        }
        return sepInput;
    }

    public void setSepInput(String sepInput) {
        this.sepInput = sepInput;
    }

    public String getDataSourcesParamsInput() {
        return dataSourcesParamsInput;
    }

    public void setDataSourcesParamsInput(String dataSourcesParamsInput) {
        this.dataSourcesParamsInput = dataSourcesParamsInput;
    }

    public String getDataSourcesFilterInput() {
        return dataSourcesFilterInput;
    }

    public void setDataSourcesFilterInput(String dataSourcesFilterInput) {
        this.dataSourcesFilterInput = dataSourcesFilterInput;
    }

    public String getDataSourcesChooseOutput() {
        return dataSourcesChooseOutput;
    }

    public void setDataSourcesChooseOutput(String dataSourcesChooseOutput) {
        this.dataSourcesChooseOutput = dataSourcesChooseOutput;
    }

    public String getDataSourceTypeOutput() {
        return dataSourceTypeOutput;
    }

    public void setDataSourceTypeOutput(String dataSourceTypeOutput) {
        this.dataSourceTypeOutput = dataSourceTypeOutput;
    }

    public String getDataSourcesTableNameOutput() {
        return dataSourcesTableNameOutput;
    }

    public void setDataSourcesTableNameOutput(String dataSourcesTableNameOutput) {
        this.dataSourcesTableNameOutput = dataSourcesTableNameOutput;
    }

    public String getDataSourcesFileNameOutput() {
        return dataSourcesFileNameOutput;
    }

    public void setDataSourcesFileNameOutput(String dataSourcesFileNameOutput) {
        this.dataSourcesFileNameOutput = dataSourcesFileNameOutput;
    }

    public String getFileTypeOutput() {
        if ("".equals(fileTypeOutput)) {
            return "csv";
        }
        return fileTypeOutput;
    }

    public void setFileTypeOutput(String fileTypeOutput) {
        this.fileTypeOutput = fileTypeOutput;
    }

    public String getEncodingOutput() {
        if ("".equals(encodingOutput)) {
            return "utf-8";
        }
        return encodingOutput;
    }

    public void setEncodingOutput(String encodingOutput) {
        this.encodingOutput = encodingOutput;
    }

    public Boolean getHeaderOutput() {
        return headerOutput;
    }

    public void setHeaderOutput(Boolean headerOutput) {
        this.headerOutput = headerOutput;
    }

    public String getSepOutput() {
        if ("".equals(sepOutput)) {
            return ",";
        }
        return sepOutput;
    }

    public void setSepOutput(String sepOutput) {
        this.sepOutput = sepOutput;
    }

    public String getDataSourcesParamsOutput() {
        return dataSourcesParamsOutput;
    }

    public void setDataSourcesParamsOutput(String dataSourcesParamsOutput) {
        this.dataSourcesParamsOutput = dataSourcesParamsOutput;
    }

    public String getColumnDatas() {
        return columnDatas;
    }

    public void setColumnDatas(String columnDatas) {
        this.columnDatas = columnDatas;
    }

    public String getDataSourcesClearOutput() {
        return dataSourcesClearOutput;
    }

    public void setDataSourcesClearOutput(String dataSourcesClearOutput) {
        this.dataSourcesClearOutput = dataSourcesClearOutput;
    }

    public List<ColumnData> getColumnDataList() {
        return columnDataList;
    }

    public void setColumnDataList(List<ColumnData> columnDataList) {
        this.columnDataList = columnDataList;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public Timestamp getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Timestamp createTime) {
        this.createTime = createTime;
    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public String getSection() {
        return section;
    }

    public void setSection(String section) {
        this.section = section;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public String getUpdateContext() {
        return updateContext;
    }

    public void setUpdateContext(String updateContext) {
        this.updateContext = updateContext;
    }

    public String getPrimaryColumns() {
        return primaryColumns;
    }

    public void setPrimaryColumns(String primaryColumns) {
        this.primaryColumns = primaryColumns;
    }

    public String getColumnSize() {
        return columnSize;
    }

    public void setColumnSize(String columnSize) {
        this.columnSize = columnSize;
    }

    public String getRowsRange() {
        return rowsRange;
    }

    public void setRowsRange(String rowsRange) {
        this.rowsRange = rowsRange;
    }

    public String getErrorRate() {
        if ("".equals(errorRate)) {
            return "0.01";
        }
        return errorRate;
    }

    public void setErrorRate(String errorRate) {
        this.errorRate = errorRate;
    }

    public String getEnableQuality() {
        if ("".equals(enableQuality)) {
            return "off";
        }
        return enableQuality.trim();
    }

    public void setEnableQuality(String enableQuality) {
        this.enableQuality = enableQuality;
    }

    public String getDuplicateColumns() {
        return duplicateColumns;
    }

    public void setDuplicateColumns(String duplicateColumns) {
        this.duplicateColumns = duplicateColumns;
    }

    public String getMerge() {
        return merge;
    }

    public void setMerge(String merge) {
        this.merge = merge;
    }
}

