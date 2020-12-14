package com.boe.examples

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

// 流处理word count
object StreamingWordCOunt {
  def main(args: Array[String]): Unit = {
    // 创建流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置任务执行并行度，默认为本地机器CPU逻辑处理器个数
    // 可以手动输入数字设定并行度，可大可小，原理是CPU核时间片轮转
    // 并行度设置为1时，可以保证数据不乱序
    env.setParallelism(1);

    val params :ParameterTool = ParameterTool.fromArgs(args)
    val host = params.get("host")
    val port = params.getInt("port")
    // 从socket获取数据流
    /*
    *  Windows 下测试脚本 nc -l -p 9999
    *  Centos 7 测试脚本 nc -lk 9999
    */
    val inputStream: DataStream[String] = env.socketTextStream(host, port)

    // 可以对任意算子设置并行度，上下游算子并行度可以不一样
    val resultStream: DataStream[(String, Int)] = inputStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    // 输出并行度设置为1，则输出不带线程序号
    resultStream.print().setParallelism(1)

    // 启动任务执行环境
    env.execute("Streaming wordcount task")

  }
}
