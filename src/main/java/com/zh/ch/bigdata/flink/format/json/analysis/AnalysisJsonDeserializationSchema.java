package com.zh.ch.bigdata.flink.format.json.analysis;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class AnalysisJsonDeserializationSchema  implements DeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;
    private static final String OP_INSERT = "I";
    private static final String OP_DELETE = "D";
    private final JsonRowDataDeserializationSchema jsonDeserializer;
    private final TypeInformation<RowData> resultTypeInfo;
    private final boolean ignoreParseErrors;
    private final int fieldCount;


    public AnalysisJsonDeserializationSchema(RowType rowType, TypeInformation<RowData> resultTypeInfo, boolean ignoreParseErrors, TimestampFormat timestampFormatOption) {
        this.resultTypeInfo = resultTypeInfo;
        this.ignoreParseErrors = ignoreParseErrors;
        this.fieldCount = rowType.getFieldCount();
        this.jsonDeserializer = new JsonRowDataDeserializationSchema(this.createJsonRowType(), resultTypeInfo, false, ignoreParseErrors, timestampFormatOption);
    }

    @Override
    public RowData deserialize(byte[] bytes) throws IOException {
        throw new RuntimeException("Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
    }

    public void deserialize(byte[] bytes, Collector<RowData> out) throws IOException {
        if (bytes != null && bytes.length != 0) {
            RowData rowData = this.jsonDeserializer.deserialize(bytes);
            String columns = rowData.getString(0).toString();
            JSONArray columnsJSONArray = JSONArray.parseArray(columns);

            String rows = rowData.getString(1).toString();
            JSONArray rowsJSONArray = JSONArray.parseArray(rows);
            for (Object object : rowsJSONArray) {
                JSONObject rowJSONObject = (JSONObject) object;
                Map<String, String> outputData = new HashMap<>();
                if (OP_INSERT.equals(rowJSONObject.getString("op_type"))) {

                    JSONArray after = rowJSONObject.getJSONArray("after");
                    int index = 0;
                    for (Object column : columnsJSONArray) {
                        outputData.put(column.toString(), after.getString(index));
                        index++;
                    }
                    GenericRowData insert = new GenericRowData(1);
                    insert.setField(0, StringData.fromBytes(JSONObject.toJSONBytes(outputData)));
                    insert.setRowKind(RowKind.INSERT);
                    out.collect(insert);
                }
                else {
                    JSONArray before = rowJSONObject.getJSONArray("before");
                    int index = 0;
                    for (Object column : columnsJSONArray) {
                        outputData.put(column.toString(), before.getString(index));
                        index++;
                    }
                    GenericRowData delete = new GenericRowData(1);
                    delete.setField(0, StringData.fromBytes(JSONObject.toJSONBytes(outputData)));
                    delete.setRowKind(RowKind.DELETE);
                    out.collect(delete);
                }
            }
        }
    }

    @Override
    public boolean isEndOfStream(RowData rowData) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return this.resultTypeInfo;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            AnalysisJsonDeserializationSchema that = (AnalysisJsonDeserializationSchema)o;
            return this.ignoreParseErrors == that.ignoreParseErrors && this.fieldCount == that.fieldCount && Objects.equals(this.jsonDeserializer, that.jsonDeserializer) && Objects.equals(this.resultTypeInfo, that.resultTypeInfo);
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(this.jsonDeserializer, this.resultTypeInfo, this.ignoreParseErrors, this.fieldCount);
    }

    private RowType createJsonRowType() {
        return (RowType) DataTypes.ROW(
                DataTypes.FIELD("columns", DataTypes.STRING()),
                DataTypes.FIELD("rows", DataTypes.STRING()),
                DataTypes.FIELD("table", DataTypes.STRING())).getLogicalType();
    }
}
