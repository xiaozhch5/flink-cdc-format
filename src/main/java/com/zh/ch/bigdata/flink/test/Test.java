package com.zh.ch.bigdata.flink.test;

import com.zh.ch.bigdata.flink.udf.AnalysisData;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Test {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);

        String q1 = "create table a1(" +
                "data string)" +
                "with (" +
                "'connector' = 'kafka'," +
                "'topic' = 'allintopic'," +
                "'properties.group.id' = 'testGroup'," +
                "'scan.startup.mode' = 'earliest-offset'," +
                "'properties.bootstrap.servers' = 'vhost-118-21:9092'," +
                "'format' = 'analysis-json')";

        String q2 = "create table a2(" +
                "col1 string," +
                "col2 string," +
                "col3 string," +
                "col4 string," +
                "col5 string," +
                "PRIMARY KEY (col1) NOT ENFORCED)" +
                "with (" +
                "'connector' = 'jdbc'," +
                "'url' = 'jdbc:mysql://hadoop:3306/cdc_test'," +
                "'table-name' = 'test_table'," +
                "'username' = 'root'," +
                "'password' = 'Pass-123-root'" +
                ")";

        String q3 = "insert into a2 select col1, col2, col3, col4, col5 from a1, LATERAL TABLE(AnalysisData(data))";

        fsTableEnv.createTemporarySystemFunction("AnalysisData", AnalysisData.class);
        fsTableEnv.executeSql(q1);
        fsTableEnv.executeSql(q2);
        TableResult tableResult = fsTableEnv.executeSql(q3);
        tableResult.await();
        fsEnv.execute();
    }
}
