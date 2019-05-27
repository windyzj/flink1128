package com.atguigu.flink1128

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._

object EventTimeApp {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //1 声明时间特性 为eventtime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val socketDstream: DataStream[String] = env.socketTextStream("hadoop1",7788)
    socketDstream.print("text==")
    val textDstream: DataStream[(String, String, Int)] = socketDstream.map(str => {

      val arr: Array[String] = str.split(" ")
      (arr(0), arr(1), 1)
    }
    )


    // 2 告知 水位 和抽取eventtime的方法
    val textWithEventTimeDstream: DataStream[(String, String, Int)] = textDstream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, String, Int)](Time.milliseconds(1000)) {
      override def extractTimestamp(element: (String, String, Int)): Long = {
        element._2.toLong
      }
    }).setParallelism(1)


    //
    //窗口必须是步长的整数倍
    val countDataStream: DataStream[(String, String, Int)] = textWithEventTimeDstream.keyBy(0).window(EventTimeSessionWindows.withGap(Time.milliseconds(2000L))).sum(2)


    countDataStream.print("window==").setParallelism(1)

    env.execute()
  }

}
