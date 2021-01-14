package FlinkWindow

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author Ljz
 * */
object WindowsApp {
  def main(args: Array[String]): Unit = {
    //    在命令行里输入nc -l -p 9999，然后输入要统计的内容

    // 创建一个流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 接受 socket 文本流
    val text: DataStream[String] = env.socketTextStream("localhost", 9999);

    //定义Tumbling Windows
    //   Tumbling windows have a fixed size and do not overlap
    //    text.flatMap(_.split(" ")) //以空格分词，得到所有的 word
    //      .map((_, 1)) //转换成 word count 二元组
    //      .keyBy(0) //按照第一个元素分组
    //      .timeWindow(Time.seconds(5))
    //      .sum(1) //按照第二个元素求和
    //      .print().setParallelism(1)

    //定义Sliding Windows
    text.flatMap(_.split(","))
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .sum(1).print().setParallelism(1)

    //上面的只是定义了处理流程，同时定义一个名称。不会让任务结束
    env.execute("WindowsApp")
  }
}
