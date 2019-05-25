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


object MyEsUtil {

  val httpHostList=new util.ArrayList[HttpHost]
  httpHostList.add(new HttpHost("hadoop1",9200))
  httpHostList.add(new HttpHost("hadoop2",9200))
  httpHostList.add(new HttpHost("hadoop3",9200))

  def getEsSink(indexName:String): ElasticsearchSink[String]  ={

    val esFunc = new ElasticsearchSinkFunction[String] {
      override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {

        val jSONObject: JSONObject = JSON.parseObject(element)

        val indexRequest: IndexRequest = Requests.indexRequest().index(indexName).`type`("_doc").source(jSONObject)
        indexer.add(indexRequest)
      }
    }



     val esSinkBuilder = new  ElasticsearchSink.Builder[String](httpHostList,esFunc)

     esSinkBuilder.setBulkFlushMaxActions(10)

     esSinkBuilder.build()

  }

}
