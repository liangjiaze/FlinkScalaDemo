package FlinkWordCount

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}

/**
 * @author Ljz
 * */
object BatchWordCount {
  def main(args: Array[String]): Unit = {
    //创建一个批处理的执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //从文件中读取数据
    var inputDateSet: DataSet[String] = env.readTextFile("E:\\bigdata\\data\\words.txt")
    //基于Dataset 做转换，首先按空格打散，然后按照 word作为key做group by
    val resultDataSet: DataSet[(String, Int)] = inputDateSet
      .flatMap(_.split(" ")) //分词得到所有 word构成的数据集
      .map((_, 1)) //_表示当前 word 转换成一个二元组（word: String,count: Int)
      .groupBy(0) //以二元组中第一个元素作为key
      .sum(1) //1表示聚合二元组的第二个元素的值

    //打印输出
    resultDataSet.print()
  }

}
