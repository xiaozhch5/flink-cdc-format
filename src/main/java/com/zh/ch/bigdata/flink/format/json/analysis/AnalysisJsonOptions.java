package com.zh.ch.bigdata.flink.format.json.analysis;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.json.JsonOptions;

public class AnalysisJsonOptions {

    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS;
    public static final ConfigOption<String> TIMESTAMP_FORMAT;
    public static final ConfigOption<String> JSON_MAP_NULL_KEY_MODE;
    public static final ConfigOption<String> JSON_MAP_NULL_KEY_LITERAL;

    public static void validateDecodingFormatOptions(ReadableConfig tableOptions) {
        JsonOptions.validateDecodingFormatOptions(tableOptions);
    }

    public static void validateEncodingFormatOptions(ReadableConfig tableOptions) {
        JsonOptions.validateEncodingFormatOptions(tableOptions);
    }

    static {
        IGNORE_PARSE_ERRORS = JsonOptions.IGNORE_PARSE_ERRORS;
        TIMESTAMP_FORMAT = JsonOptions.TIMESTAMP_FORMAT;
        JSON_MAP_NULL_KEY_MODE = JsonOptions.MAP_NULL_KEY_MODE;
        JSON_MAP_NULL_KEY_LITERAL = JsonOptions.MAP_NULL_KEY_LITERAL;
    }

}
