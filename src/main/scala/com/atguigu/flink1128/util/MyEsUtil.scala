package com.atguigu.flink1128.util

import java.util

import com.alibaba.fastjson.serializer.{SerializerFeature, ToStringSerializer}
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.flink1128.bean.StartUpLog
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.json4s.native.JsonMethods
import org.json4s.JsonDSL._

object MyEsUtil {

  val httpHostList=new util.ArrayList[HttpHost]
  httpHostList.add(new HttpHost("hadoop1",9200))
  httpHostList.add(new HttpHost("hadoop2",9200))
  httpHostList.add(new HttpHost("hadoop3",9200))

  def getEsSink(indexName:String): ElasticsearchSink[StartUpLog]  ={

    val esFunc = new ElasticsearchSinkFunction[StartUpLog] {
      override def process(element: StartUpLog, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
        val list: List[(String, Long)] = List[(String, Long)]()
        implicit val formats = org.json4s.DefaultFormats
        val jsonstr2: String = org.json4s.native.Serialization.write(element)
     //   val jsonStr: String = JSON.toJSONString(element,SerializerFeature.)
//
        val jSONObject: JSONObject = JSON.parseObject(jsonstr2)
         println(element)


        val indexRequest: IndexRequest = Requests.indexRequest().index(indexName).`type`("_doc").source(element)
        indexer.add(indexRequest)
      }
    }



     val esSinkBuilder = new  ElasticsearchSink.Builder[StartUpLog](httpHostList,esFunc)

     esSinkBuilder.setBulkFlushMaxActions(10)

     esSinkBuilder.build()

  }

}
