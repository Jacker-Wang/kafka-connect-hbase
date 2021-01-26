package io.svectors.hbase;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author wangpin
 * @version 1.0
 * @date 2021/1/26 0026
 * @description
 */
public class Test {
    private static String  bootstrapServers="bdmp-31:6667,bdmp-40:6667,bdmp-42:6667";

    /**
     * 创建 KafkaProducer 客户端对象
     */
    private static KafkaProducer<String, String> createKafkaProducer() {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(prop);
    }

    public static  void main(String [] args){
        String key="{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"}],\"optional\":false,\"name\":\"tree722__transformer__6566_1611645429742.jacker.jacker_person.Key\"},\"payload\":{\"id\":9}}";
        String value="{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"age\"},{\"type\":\"string\",\"optional\":false,\"field\":\"date\"}],\"optional\":true,\"name\":\"tree722__transformer__6566_1611645429742.jacker.jacker_person.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"age\"},{\"type\":\"string\",\"optional\":false,\"field\":\"date\"}],\"optional\":true,\"name\":\"tree722__transformer__6566_1611645429742.jacker.jacker_person.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"server_id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"gtid\"},{\"type\":\"string\",\"optional\":false,\"field\":\"file\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"pos\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"row\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"thread\"},{\"type\":\"string\",\"optional\":true,\"field\":\"query\"}],\"optional\":false,\"name\":\"io.debezium.connector.mysql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"total_order\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"data_collection_order\"}],\"optional\":true,\"field\":\"transaction\"}],\"optional\":false,\"name\":\"tree722__transformer__6566_1611645429742.jacker.jacker_person.Envelope\"},\"payload\":{\"before\":null,\"after\":{\"id\":9,\"name\":\"test7\",\"age\":4444,\"date\":\"fdss\"},\"source\":{\"version\":\"1.3.1.Final\",\"connector\":\"mysql\",\"name\":\"tree722__transformer__6566_1611645429742\",\"ts_ms\":0,\"snapshot\":\"true\",\"db\":\"jacker\",\"table\":\"jacker_person\",\"server_id\":0,\"gtid\":null,\"file\":\"mysql-bin.000003\",\"pos\":781222107,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"c\",\"ts_ms\":1611645439621,\"transaction\":null}}";

        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("jacker", key,value);
        Future<RecordMetadata> send = kafkaProducer.send(producerRecord);
        System.out.println(send.isDone());

        try {
            TimeUnit.DAYS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}