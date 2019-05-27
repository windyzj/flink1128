package com.atguigu.flink1128.util

import java.sql.{Connection, PreparedStatement}
import java.util.Properties
import javax.sql.DataSource

import com.alibaba.druid.pool.DruidDataSourceFactory
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

class MyJdbcSink(sql:String)  extends  RichSinkFunction[Array[Any]]{

  val driver="com.mysql.jdbc.Driver"

  val url="jdbc:mysql://hadoop2:3306/gmall1128?useSSL=false"

  val username="root"

  val password="123123"

  val maxActive="20"

  var connection:Connection=null;


  override def open(parameters: Configuration): Unit ={

    val properties = new Properties()
    properties.put("driverClassName",driver)
    properties.put("url",url)
    properties.put("username",username)
    properties.put("password",password)
    properties.put("maxActive",maxActive)


     val dataSource: DataSource = DruidDataSourceFactory.createDataSource(properties)
     connection= dataSource.getConnection
  }

  override def invoke(values: Array[Any]): Unit = {
    val preparedStatement: PreparedStatement = connection.prepareStatement(sql)

    for ( i<- 0 to values.size-1   ) {
      preparedStatement.setObject( i+1,values(i) )
    }

    preparedStatement.executeUpdate()
  }

  override def close(): Unit = {
    if(connection!=null){
      connection.close()
    }
  }


}
