package com.atguigu.flink1128

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
object StreamWcApp {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dstream: DataStream[String] = env.socketTextStream("hadoop1",7777)

    val wcDstream: DataStream[(String, Int)] = dstream.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1)

    wcDstream.print().setParallelism(1)

    env.execute()
  }

}
