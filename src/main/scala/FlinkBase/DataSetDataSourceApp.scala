package FlinkBase

import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * @author Ljz
 * */
object DataSetDataSourceApp {
  def main(args: Array[String]): Unit = {
    val executionEnvironment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    fromCollection(executionEnvironment)
  }

  def fromCollection(env: ExecutionEnvironment): Unit = {
    /*从集合创建dataset*/
    //    val data = 1 to 10
    //    env.fromCollection(data).print()

    /*从文件创建dataset*/
    //    val filePath = "file:///Users/thpffcj/Public/data/hello.txt"
    val filePath = "E:\\bigdata\\data\\words.txt"
    val data: Unit = env.readTextFile(filePath).print()

    /*从csv文件创建dataset*/
    //    val filePath = "E:\\bigdata\\data\\tg_ppq&upq01.csv"
    //    val data = env.readCsvFile[MyCaseClass](filePath, ignoreFirstLine = true, includedFields = Array(0, 4))


    //    env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    //      .map(_ + 1)
    //      .filter(_ > 5).first(3).print()

  }

  case class MyCaseClass(TG_ID: String, STAT_DATE: String, PPQ: Int, UPQ: Int, LINELOSS_RATE: Double)

}
