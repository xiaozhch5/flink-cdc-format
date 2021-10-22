package com.zh.ch.bigdata.flink.format.json.analysis;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class AnalysisJsonFormatFactory  implements DeserializationFormatFactory, SerializationFormatFactory {

    public static final String IDENTIFIER = "analysis-json";

    public AnalysisJsonFormatFactory() {
    }

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        AnalysisJsonOptions.validateDecodingFormatOptions(formatOptions);
        boolean ignoreParseErrors = (Boolean)formatOptions.get(AnalysisJsonOptions.IGNORE_PARSE_ERRORS);
        TimestampFormat timestampFormat = JsonOptions.getTimestampFormat(formatOptions);
        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context, DataType dataType) {
                // 表的字段名和数据类型
                RowType rowType = (RowType) dataType.getLogicalType();
                TypeInformation<RowData> rowDataTypeInformation = context.createTypeInformation(dataType);
                return new AnalysisJsonDeserializationSchema(rowType, rowDataTypeInformation, ignoreParseErrors, timestampFormat);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT).addContainedKind(RowKind.DELETE).build();
            }
        };
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(DynamicTableFactory.Context context, ReadableConfig readableConfig) {

        return null;
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(AnalysisJsonOptions.IGNORE_PARSE_ERRORS);
        return options;
    }
}
