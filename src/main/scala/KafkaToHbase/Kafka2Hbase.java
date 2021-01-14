package KafkaToHbase;

import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @author ljz
 */
public class Kafka2Hbase {

    public static void main(String[] args) throws Exception {

        // 引入Flink StreamExecutionEnvironment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置监控数据流时间间隔（官方叫状态与检查点）
        env.enableCheckpointing(1000);

        // 配置kafka和zookeeper的ip和端口
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "140.246.204.127:9092,172.29.0.246:9092");
        properties.setProperty("group.id", "test");


        // 记载kafka和zookeeper的配置
        FlinkKafkaConsumer011 <String> myConsumer = new FlinkKafkaConsumer011 <>("test", new SimpleStringSchema(), properties);

        myConsumer.setStartFromEarliest();     // start from the earliest record possible

//        myConsumer.setStartFromLatest();       // start from the latest record
//        myConsumer.setStartFromTimestamp(...); // start from specified epoch timestamp (milliseconds)
//        myConsumer.setStartFromGroupOffsets(); // the default behaviour
        /*
        // 指定位置
        Map <KafkaTopicPartition, Long> specificStartOffsets = new HashMap <>();
        specificStartOffsets.put(new KafkaTopicPartition("myTopic", 0), 23L);
        myConsumer.setStartFromSpecificOffsets(specificStartOffsets);
*/

//         转换kafka数据类型为flink的dataStream类型
        DataStreamSource stream = env.addSource(myConsumer);

        /* 打印数据内容 */
        stream.print();

        //写入hbase
        stream.addSink(new HBaseWriter());


        // 注册运行名称
        env.execute("the data from kafka");
    }


}