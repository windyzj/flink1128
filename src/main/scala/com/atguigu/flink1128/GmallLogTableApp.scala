package com.atguigu.flink1128

import com.alibaba.fastjson.JSON
import com.atguigu.flink1128.bean.StartUpLog
import com.atguigu.flink1128.util.MykafkaUtil
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

object GmallLogTableApp {


  def main(args: Array[String]): Unit = {
      val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

      val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

      val consumer: FlinkKafkaConsumer011[String] = MykafkaUtil.getKafkaConsumer("GMALL_STARTUP")
      val startupJsonDstream: DataStream[String] = env.addSource(consumer)

      val startupLogDSream: DataStream[StartUpLog] = startupJsonDstream.map(jsonstr=>JSON.parseObject(jsonstr,classOf[StartUpLog]))

      val startupLogWithETDstream: DataStream[StartUpLog] = startupLogDSream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[StartUpLog](Time.milliseconds(0)) {
          override def extractTimestamp(element: StartUpLog): Long = {
              element.ts
          }
      })

      //声明 时间字段
      val startupTable: Table = tableEnv.fromDataStream(startupLogWithETDstream,'mid,'uid,'appid,'area,'os,'ch,'logType,'vs,'logDate,'logHour,'logHourMinute,'ts.rowtime)

      //val filteredTable: Table = startupTable.select('mid,'ch).filter("ch = 'appstore'")
      //val resultDstream: DataStream[(String, String)] = filteredTable.toAppendStream[(String,String)]
      // 计算每10秒钟（开窗）按eventtime进行开窗 各个channel的个数（keyby）

      //val chCountTable: Table = startupTable.window(Tumble over 10000.millis on 'ts as 'tt).groupBy('ch,'tt).select('ch,'ch.count)
      val chCountSQLTable: Table = tableEnv.sqlQuery("select  ch ,count(ch) from "+startupTable+ " group by ch ,TUMBLE(ts, INTERVAL '10' SECOND)")


      //经过聚合的结果 转流要用 toRetract
      val chCountDStream: DataStream[(Boolean, (String, Long))] = chCountSQLTable.toRetractStream[(String,Long)]

      chCountDStream.print()

      env.execute()

  }

}
