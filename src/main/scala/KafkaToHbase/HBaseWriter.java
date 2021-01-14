package KafkaToHbase;

/**
 * @author Ljz
 **/
//import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Array;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * 写入HBase
 * 继承RichSinkFunction重写父类方法
 *
 * 写入hbase时500条flush一次, 批量插入, 使用的是writeBufferSize
 */
class HBaseWriter extends RichSinkFunction<DeviceData>{

    private static org.apache.hadoop.conf.Configuration configuration;
    private static Connection connection = null;
    private static BufferedMutator mutator;
    private static int count = 0;

    @Override
    public void open(Configuration parameters) throws Exception {
        configuration = HBaseConfiguration.create();
//        configuration.set("hbase.master", "140.246.129.160:8020");
        configuration.set("hbase.zookeeper.quorum", "113.125.5.131");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf("t1"));
        params.writeBufferSize(2 * 1024 * 1024);
        mutator = connection.getBufferedMutator(params);
    }

    @Override
    public void close() throws IOException {
        if (mutator != null) {
            mutator.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    /**
     * 处理获取的hbase数据
     * @param value
     * @param context
     */

    public void invoke(String value, Context context) throws Exception {
        String cf1 = "cf1";
        String[] array  = value.split(",");
        Put put = new Put(Bytes.toBytes(String.valueOf(array[0])));
        put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("name"), Bytes.toBytes(array[1]));
        put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("age"), Bytes.toBytes(array[2]));
        mutator.mutate(put);
        //每满2000条刷新一下数据
        if (count >= 2000){
            mutator.flush();
            count = 0;
        }
        count = count + 1;
    }


    /**
    @Override
    public void invoke(DeviceData values, Context context) throws Exception {
        //Date 1970-01-06 11:45:55  to 445555000
        long unixTimestamp= 0;
        try {
            String gatherTime = values.GatherTime;
            //毫秒和秒分开处理
            if (gatherTime.length() > 20) {
                long ms = Long.parseLong(gatherTime.substring(20, 23));
                Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(gatherTime);
                unixTimestamp = date.getTime() + ms;
            } else {
                Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(gatherTime);
                unixTimestamp = date.getTime();
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        String rowkey = values.MachID + String.valueOf(unixTimestamp);
        String key = values.OperationValue;
        String value = values.OperationData;
        System.out.println("Column Family=f1,  RowKey=" + rowkey + ", Key=" + key + " ,Value=" + value);
        Put put = new Put(rowkey.getBytes());
        put.addColumn("f1".getBytes(), key.getBytes(), value.getBytes());
        mutator.mutate(put);
        //每满500条刷新一下数据
        if (count >= 500){
            mutator.flush();
            count = 0;
        }
        count = count + 1;
    }

    **/


}



