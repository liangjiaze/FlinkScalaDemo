package FlinkTable

/**
 * @author Ljz
 * */
object FlinkTableSql {
  def main(args: Array[String]): Unit = {
    //
    //    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //    val tableEnv: scala.BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)
    //
    //    val filePath =  "E:\\bigdata\\data\\tg_ppq&upq.csv"
    //    // 已经拿到DataSet
    //    val csv: DataSet[SalesLog] = env.readCsvFile[SalesLog](filePath, ignoreFirstLine = true)
    //
    //    // DataSet => Table
    //    val salesTable: Table = tableEnv.fromDataSet(csv)
    //    // Table => table
    //    tableEnv.registerTable("sales", salesTable)
    //
    //    // sql
    //    val resultTable: Table = tableEnv.sqlQuery("select * from sales")
    //
    //    val count: Long =tableEnv.toDataSet[Row](resultTable).count()
    //    print(count)

  }

  case class SalesLog(STAT_DATE: String, PPQ: Long, UPQ: Long, LINELOSS_RATE: Double)

}
