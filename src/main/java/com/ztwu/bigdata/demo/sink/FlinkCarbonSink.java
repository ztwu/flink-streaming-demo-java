package com.ztwu.bigdata.demo.sink;

import com.ztwu.bigdata.demo.util.KafkaUtil;
import org.apache.carbon.flink.CarbonLocalProperty;
import org.apache.carbon.flink.CarbonWriterFactory;
import org.apache.carbon.flink.ProxyFileSystem;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Flink-Sink-Carbon
 */
public class FlinkCarbonSink {

    private static Logger LOGGER = LoggerFactory.getLogger(FlinkCarbonSink.class);

    public static void main(String[] args) {
        boolean isOnline = true;
        LOGGER.info("begin to init flink");
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        bsEnv.enableCheckpointing(300000L);
        bsEnv.setRestartStrategy(RestartStrategies.fallBackRestart());
        LOGGER.info("flink with (%s)", bsEnv.getConfig().toString());

        final Properties kfkProperties = KafkaUtil.makeKafkaProperties(isOnline);
        kfkProperties.setProperty("group.id", "flink_to_carbon_real");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(KafkaUtil.KAFKA_TOPIC,
                new SimpleStringSchema(), kfkProperties);
        DataStream<String> source = bsEnv.addSource(kafkaConsumer);

        // 写入到carbondata文件当中
        CarbonWriterFactory writerFactory = getCarbonWriterFactory(isOnline, "database", "table");
        DataStream<String[]> records = source.filter(
            (FilterFunction<String>) value -> StringUtils.isBlank(value) ? false : true)
            .map((MapFunction<String, String[]>) value -> value.split(","));
        StreamingFileSink streamSink = StreamingFileSink.forBulkFormat(new Path(ProxyFileSystem.DEFAULT_URI), writerFactory).build();
        records.addSink(streamSink);
        try {
            bsEnv.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static CarbonWriterFactory getCarbonWriterFactory(boolean isOnline, String databaseName, String tableName) {
        String tablePath = "hdfs://***:8020/user/hive/warehouse/";
        if (isOnline) {
            tablePath = "hdfs://***/user/carbon/warehouse/";
        }
        String dataTempPath = System.getProperty("java.io.tmpdir") + "/";
        Properties tableProperties = new Properties();
        Properties writerProperties = new Properties();
        writerProperties.setProperty(CarbonLocalProperty.DATA_TEMP_PATH, dataTempPath);
        writerProperties.setProperty("carbon.writer.local.commit.threshold", "1500000");
        Properties carbonProperties = new Properties();
        // carbonProperties.setProperty(CarbonCommonConstants.STORE_LOCATION, "hdfs:///user/hive/warehouse");
        // carbonProperties.setProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
        // carbonProperties.setProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT);
        // carbonProperties.setProperty(CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB, "1024");
        return CarbonWriterFactory.builder("Local").build(
                databaseName,
                tableName,
                tablePath,
                tableProperties,
                writerProperties,
                carbonProperties
        );
    }
}
