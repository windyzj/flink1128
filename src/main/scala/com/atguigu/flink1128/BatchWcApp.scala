package com.atguigu.flink1128

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

object BatchWcApp {


  def main(args: Array[String]): Unit = {
    val parameterTool: ParameterTool = ParameterTool.fromArgs(args)

    val inputPath: String = parameterTool.get("input")
    val outputPath: String = parameterTool.get("output")

     //  env => source => transform=> sink
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //source
    val dataSet: DataSet[String] = env.readTextFile(inputPath)

    dataSet.print()
    //transform
   //  val aggDataSet: AggregateDataSet[(String, Int)] = dataSet.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)

    //sink
    //aggDataSet.writeAsCsv(outputPath).setParallelism(1)

    //env.execute()



  }

}
