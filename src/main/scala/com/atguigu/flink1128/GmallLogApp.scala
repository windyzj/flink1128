package com.atguigu.flink1128

import com.alibaba.fastjson.JSON
import com.atguigu.flink1128.bean.StartUpLog
import com.atguigu.flink1128.util.{MyEsUtil, MyJdbcSink, MyRedisUtil, MykafkaUtil}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, KeyedStream, SplitStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.redis.RedisSink

object GmallLogApp {

  def main(args: Array[String]): Unit = {

      val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

      val consumer: FlinkKafkaConsumer011[String] = MykafkaUtil.getKafkaConsumer("GMALL_STARTUP")
      val startupDstream: DataStream[String] = env.addSource(consumer)
 //   startupDstream.print()

//    val startupCountDstream: DataStream[(String, Int)] = startupDstream.map { jsonString =>
//      val startUpLog: StartUpLog = JSON.parseObject(jsonString, classOf[StartUpLog])
//
//      (startUpLog.ch, 1)
//    }
//     val groupbyChStream: KeyedStream[(String, Int), Tuple] = startupCountDstream.keyBy(0)


//
//    val chCountDstream: DataStream[(String, Int)] = groupbyChStream.reduce { (ch1, ch2) =>
//      val ch: String = ch1._1
//      (ch, ch1._2 + ch2._2)
//    }


    val startupLogDstream: DataStream[StartUpLog] = startupDstream.map { jsonString =>
      val startUpLog: StartUpLog = JSON.parseObject(jsonString, classOf[StartUpLog])
      startUpLog
    }
    val splitBychStream: SplitStream[StartUpLog] = startupLogDstream.split { startupLog =>
      var flag: List[String] = null;
      if (startupLog.ch == "appstore") {
        flag = List("usa")
      } else {
        flag = List("china")
      }
      flag
    }
    val usaStream: DataStream[StartUpLog] = splitBychStream.select("usa")
    val chinaStream: DataStream[StartUpLog] = splitBychStream.select("china")


    //usaStream.print("usa::::").setParallelism(1)
    //chinaStream.print("china::::").setParallelism(1)

    val connStream: ConnectedStreams[StartUpLog, StartUpLog] = usaStream.connect(chinaStream)
    val coMapDstream: DataStream[String] = connStream.map(
      (startlog1: StartUpLog) => "usa::" + startlog1.ch,
      (startlog2: StartUpLog) => "china::" + startlog2.ch
    )
   // coMapDstream.print("all==>>>")


//    val unionStream: DataStream[StartUpLog] = usaStream.union(chinaStream)
//    unionStream.print("union==>>>")


//   val channelProducer: FlinkKafkaProducer011[String] = MykafkaUtil.getKafkaProducer("channel_from")
//    coMapDstream.addSink(channelProducer)
//
    // 统计中国美国各有多少个  并保存到redis中
    val groupbyCountryDstream: DataStream[(String, Int)] = coMapDstream.map(_.split("::")(0)).map((_,1)).keyBy(0).sum(1)
   // groupbyCountryDstream.print()

    //保存redis
//    val redisSink: RedisSink[(String, Int)] = MyRedisUtil.getRedisSink()
//    groupbyCountryDstream.addSink(redisSink)

    //把明细保存到ES 中
   // val esSink: ElasticsearchSink[StartUpLog] = MyEsUtil.getEsSink("gmall1128_startup_flink2")
   // startupLogDstream.print()
    //startupLogDstream.addSink(esSink)


    val myJdbcSink = new MyJdbcSink("insert into  z_startup  values(?,?,?,?,?)")
    val startupForJdbcDstream: DataStream[Array[Any]] = startupLogDstream.map(startuplog=>Array(startuplog.mid,startuplog.ch,startuplog.area,startuplog.vs,startuplog.uid))
    startupForJdbcDstream.addSink(myJdbcSink)


    // 每10秒钟统计各个渠道的个数
    val startupCountDstream: DataStream[(String, Int)] = startupDstream.map { jsonString =>
      val startUpLog: StartUpLog = JSON.parseObject(jsonString, classOf[StartUpLog])

      (startUpLog.ch, 1)
    }
    val groupbyChStream: KeyedStream[(String, Int), Tuple] = startupCountDstream.keyBy(0)
    //val windowStream: WindowedStream[(String, Int), Tuple, TimeWindow] = groupbyChStream.timeWindow(Time.seconds(10L),Time.seconds(2L))

    val windowStream: WindowedStream[(String, Int), Tuple, GlobalWindow] = groupbyChStream.countWindow(10,2)

    val chCountByWindowDstream: DataStream[(String, Int)] = windowStream.sum(1)

    chCountByWindowDstream.print()
    env.execute()

  }

}
