package com.boe.examples

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

object WordCount {

  def main(args: Array[String]): Unit = {
    // 创建一个批处理上下文执行环境
    val env : ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment ;

    // 从文件中读取数据
    val inputFilePath:String = "C:\\Users\\wanghuan\\Desktop\\FlinkTutorial\\src\\main\\resources\\words.txt";
    val inputDSData:DataSet[String] = env.readTextFile(inputFilePath) ;

    // 对数据进行切分统计，输出结果为二元组(key,count)
    // :DataSet[(String,Int)]
    val resultDSData = inputDSData
      .flatMap(_.split(" ")) // 按照空格进行切分
      .map((_,1)) // 切分后使用map生成每个单词二元组
      .groupBy(0) // 按照二元组中的第一个元素group by
      .sum(1); //  按照二元组中的第2个元素sum 求和

    // 打印结果
    resultDSData.print()
  }

}
