package com.cansever.consumer;

import com.cansever.consumer.message.MessageObject;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * User: TTACANSEVER
 */
public class BackupMessageConsumer {

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.out.println("Wrong arguments!  1: config file");
            System.exit(-1);
        }

        String confFile = args[0];

        final Properties applicationProperties = readProperties(new File(confFile));
        if(applicationProperties == null) {
            System.out.println("Properties file parse problem!");
            System.exit(-1);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing();

        FlinkKafkaConsumer082 consumer = new FlinkKafkaConsumer082<>(applicationProperties.getProperty("kafka.message.backup.topic"),
                                                                     new AvroDeserializationSchema(),
                                                                     filterKafkaProperties(applicationProperties));
        DataStream<MessageObject> messageStream = env.addSource(consumer);

        String cassandraHosts = applicationProperties.getProperty("cassandra.hosts");
        int cassandraPort = Integer.parseInt(applicationProperties.getProperty("cassandra.port"));
        String cassandraUsername = applicationProperties.getProperty("cassandra.username");
        String cassandraPassword = applicationProperties.getProperty("cassandra.password");
        String cassandraKeyspace = applicationProperties.getProperty("cassandra.keyspace");
        int ttl = -1;
        try {
            ttl = Integer.parseInt(applicationProperties.getProperty("record.ttl"));
        } catch (Exception e) {

        }

        messageStream
                .writeUsingOutputFormat(
                        new CassandraOutputFormat(
                                cassandraHosts,
                                cassandraPort,
                                cassandraUsername,
                                cassandraPassword,
                                cassandraKeyspace,
                                ttl)).name("Cassandra Writer");

        env.execute(applicationProperties.getProperty("job.name"));
    }

    private static Properties readProperties(File confFile) throws IOException {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(confFile));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return properties;
    }

    private static Properties filterKafkaProperties(Properties applicationProperties){
        Properties kafkaProperties = new Properties();
        for(Object keyObj : applicationProperties.keySet()) {
            String key = (String) keyObj;
            String value = (String) applicationProperties.get(key);
            if(key.startsWith("kafka.message.backup.")) {
                kafkaProperties.setProperty(key.replace("kafka.message.backup.", ""), value);
            }
        }
        return kafkaProperties;
    }
}
