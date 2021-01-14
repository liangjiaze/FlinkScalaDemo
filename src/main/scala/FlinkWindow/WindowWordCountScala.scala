package FlinkWindow

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 滑动窗口
 * 每隔1秒钟统计最近2秒内的数据，打印到控制台。
 * */

object WindowWordCountScala {
  def main(args: Array[String]): Unit = {
    //    在命令行里输入nc -l -p 9809，然后输入要统计的内容

    //获取参数
    val hostname = ParameterTool.fromArgs(args).get("hostname")
    val port = ParameterTool.fromArgs(args).getInt("port")
    //导入隐式转换
    import org.apache.flink.api.scala._
    //步骤一：获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //步骤二：获取数据源
    val textStream = env.socketTextStream(hostname, port)
    //步骤三：数据处理
    val wordCountStream = textStream.flatMap(line => line.split(","))
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(2), Time.seconds(1))
      .sum(1)
    //步骤四：数据结果处理
    wordCountStream.print()
    //步骤六：启动程序
    env.execute("WindowWordCountScala")
  }

}
