package com.waiter.sparkstreaming;


import com.waiter.sparkstreaming.dao.BaseDAO;
import com.waiter.sparkstreaming.entity.Stat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Date;

/**
 * @ClassName NetworkWordCount
 * @Description TOOD
 * @Author Waiter
 * @Date 2020/5/25 21:49
 * @Version 1.0
 */
public class NginxLogProcess {
    //Spark Streaming处理的时间批次，单位为ms，这里以10s为一个批次
    private static final int BATCH = 10000;

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("NginxArray");
        //在本地虚拟机中启动两个线程运行Spark(用于本地执行Main方法时调试)
//        sparkConf.setMaster("local[2]");

        //指定Spark的Master节点
        sparkConf.setMaster("spark://192.168.121.134:7077");

        //获取Java环境下的SparkStreaming运行环境
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(BATCH));

        //从Socket读取数据(用于本地调试，在服务器上使用nc -lk 9999命令开启9999端口，然后就能够在服务器上往这个端口发送数据)
//        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("192.168.121.134", 9999);

        //从HDFS的指定路径获取日志
        //日志格式：10.202.143.88 - - [2020-05-26 21:23:06] "GET /login.php HTTP/1.1" 200 0 "-" "HTC_Dream Mozilla/5.0 (Linux; U; Android 1.5; en-ca; Build/CUPCAKE) AppleWebKit/528.5  (KHTML, like Gecko) Version/3.1.2 Mobile Safari/525.20.1" "-"
        JavaDStream<String> lines = ssc.textFileStream("hdfs://192.168.121.134:9000/spark/streaming");

        //先进行Map操作，获取(ip,1)类型的Tuple
        JavaPairDStream<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                //s.split(" ")[0]获取的日志中的IP
                return new Tuple2<String, Integer>(s.split(" ")[0], 1);
            }
        });
        //进行Reduce操作，将IP相同的Tuple进行合并
        JavaPairDStream<String, Integer> ipPv = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }).transformToPair(new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
            public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> rdd) throws Exception {
                JavaPairRDD<String, Integer> temp1 = rdd.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                        return new Tuple2<Integer, String>(tuple2._2, tuple2._1);
                    }
                }).sortByKey().mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
                    public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple2) throws Exception {
                        return new Tuple2<String, Integer>(tuple2._2, tuple2._1);
                    }
                });
                return temp1;
            }
        });
        //获取UV，并插入到数据库中
        ipPv.count().foreachRDD(new VoidFunction<JavaRDD<Long>>() {
            @Override
            public void call(JavaRDD<Long> rdd) throws Exception {
                rdd.foreach(new VoidFunction<Long>() {
                    @Override
                    public void call(Long aLong) throws Exception {
                        String insertSQL = "INSERT INTO stat(uv,create_datetime) VALUES(?,?)";
                        Object[] insertParams = {aLong.toString(),new Date()};
                        BaseDAO.modifyObj(insertSQL, insertParams);
                    }
                });
            }
        });

        //启动服务
        ssc.start();
        ssc.awaitTermination();
    }
}
