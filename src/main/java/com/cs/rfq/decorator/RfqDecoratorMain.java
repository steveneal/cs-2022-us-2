package com.cs.rfq.decorator;

import io.netty.handler.codec.string.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RfqDecoratorMain {

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\Java\\hadoop-2.9.2");
        System.setProperty("spark.master", "local[4]");

        //TODO: create a Spark configuration and set a sensible app name
        SparkConf conf = new SparkConf().setAppName("TradeDataPuller");

        //TODO: create a Spark streaming context
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));


        //TODO: create a Spark session
        SparkSession session = SparkSession.builder().config(conf).getOrCreate();


        KafkaProcessor processor = new KafkaProcessor(session, jssc);

        JavaDStream<String> stream = processor.initRfqStream();

        //.map(Rfq::fromJson)

        RfqProcessor processorRfq = new RfqProcessor(session, jssc);
        stream.foreachRDD(rdd -> {
                    rdd.collect().stream().map(r -> {
                        Rfq rfq = Rfq.fromJson(r);
                        processorRfq.processRfq(rfq);
                        return rfq;
                    }).forEach(System.out::println);
                }
        );

        //TODO: create a new RfqProcessor and set it listening for incoming RFQs
//        RfqProcessor rfqProc = new RfqProcessor(session, jssc);
//        rfqProc.startSocketListener();
        System.out.println("Listening for RFQ's");
        jssc.start();
        jssc.awaitTermination();
    }

}
