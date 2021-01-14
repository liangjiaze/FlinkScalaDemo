package Flink_Kafka

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.slf4j.LoggerFactory

/**
  * @author Ljz
  **/
object LogAnalysis {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    /*
       在nn02上手动添加kafka消费者的指令：
         /opt/cloudera/parcels/KAFKA/bin/kafka-console-consumer --bootstrap-server PLAINTEXT://140.246.204.127:9092 --topic test --from-beginning
        */

    // 指定kafka主题
    val topic = "test"

    // 配置kafka的主机和消费群组
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "PLAINTEXT://140.246.204.127:9092")
    properties.setProperty("group.id", "test")

    // 构建FlinkKafkaConsumer
    val myConsumer = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), properties)
    // 设置偏移量
    myConsumer.setStartFromEarliest() // start from the earliest record possible
    //    myConsumer.setStartFromLatest()        // start from the latest record
    //    myConsumer.setStartFromTimestamp(...)  // start from specified epoch timestamp (milliseconds)
    //    myConsumer.setStartFromGroupOffsets()  // the default behaviour

    //指定位置
    //val specificStartOffsets = new java.util.HashMap[KafkaTopicPartition, java.lang.Long]()
    //specificStartOffsets.put(new KafkaTopicPartition("myTopic", 0), 23L)
    //myConsumer.setStartFromSpecificOffsets(specificStartOffsets)

    val stream: DataStream[String] = env.addSource(myConsumer)

    env.enableCheckpointing(5000)

//    stream.print()

    // 在生产上记录日志建议采用这种方式
    val logger = LoggerFactory.getLogger("LogAnalysis")

    // 接收kafka数据
    val logData = stream.map(x => {
      val splits = x.split("\t")
      val level = splits(2)
      val timeStr = splits(3)
      var time = 0l

      try {
        val sourceFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        time = sourceFormat.parse(timeStr).getTime
      } catch {
        case e: Exception =>
          logger.error(s"time parse error: $timeStr", e.getMessage)
      }

      val domain = splits(5)
      val traffic = splits(6).toLong

      (level, time, domain, traffic)

    }).filter(_._2 != 0)
      .filter(_._1 == "E")
      .map(x=>{(x._2,x._3,x._4)})

    logData.print()

    env.execute("LogAnalysis")
  }
}
