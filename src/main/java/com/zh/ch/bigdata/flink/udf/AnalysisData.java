package com.zh.ch.bigdata.flink.udf;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

@FunctionHint(output = @DataTypeHint("ROW<col1 STRING, col2 STRING, col3 STRING, col4 STRING, col5 STRING>"))
public class AnalysisData extends TableFunction<Row> {
    public void eval(String str) {
        JSONObject data = JSONObject.parseObject(str);
        collect(Row.of(
                data.getString("col1"),
                data.getString("col2"),
                data.getString("col3"),
                data.getString("col4"),
                data.getString("col5")));
    }
}
