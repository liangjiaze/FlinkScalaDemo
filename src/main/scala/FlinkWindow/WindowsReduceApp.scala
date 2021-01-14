package FlinkWindow

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author Ljz
 * */
object WindowsReduceApp {
  def main(args: Array[String]): Unit = {
    //    在命令行里输入nc -l -p 9990，然后输入要统计的内容

    // 创建一个流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 接受 socket 文本流
    val text = env.socketTextStream("localhost", 9990);

    text.flatMap(_.split(","))
      .map(x => (1, x.toInt))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .reduce((v1, v2) => {
        println(v1 + " + " + v2)
        (v1._1, v1._2 + v2._2)
      }).print().setParallelism(1)

    //上面的只是定义了处理流程，同时定义一个名称。不会让任务结束
    env.execute("WindowsReduceApp")
  }
}
