package com.atguigu.flink1128.util

import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object MyRedisUtil {

  private val jedisPoolConfig: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("hadoop1").setPort(6379).build()

  def getRedisSink(): RedisSink[(String,Int)] ={
        new RedisSink[(String,Int)](jedisPoolConfig,new MyRedisMapper)
  }


  class MyRedisMapper extends  RedisMapper[(String,Int)]{
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET,"country_count")
    }

    override def getValueFromData(t: (String, Int)): String =  t._2.toString

    override def getKeyFromData(t: (String, Int)): String = t._1


  }

}
