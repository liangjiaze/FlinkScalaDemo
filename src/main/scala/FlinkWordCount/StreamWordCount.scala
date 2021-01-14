package FlinkWordCount

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}

/**
 * @author Ljz
 * */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //    在命令行里输入nc -l -p 9000，然后输入要统计的内容

    // 创建一个流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 接受 socket 文本流
    val inputDataStream: DataStream[String] = env.socketTextStream("localhost", 9000);

    /*// 接受 socket 文本流  hostname:prot 从程序运行参数中读取
    val params: ParameterTool = ParameterTool.fromArgs(args);
    val hostname: String = params.get("host");
    val port: Int = params.getInt("port");
    val inputDataStream: DataStream[String] = env.socketTextStream(hostname, port);
    //定义转换操作 word count*/

    //定义转换操作 word count
    val resultDataStream = inputDataStream
      .flatMap(_.split(" ")) //以空格分词，得到所有的 word
      .filter(_.nonEmpty)
      .map(k => WordWithCount(k, 1)) //转换成 word count 二元组
      .keyBy("word") //按照第一个元素分组
      .sum(1) //按照第二个元素求和

    resultDataStream.print()

    //上面的只是定义了处理流程，同时定义一个名称。不会让任务结束
    env.execute("stream word count word")
  }

  case class WordWithCount(word: String, count: Long)

}
