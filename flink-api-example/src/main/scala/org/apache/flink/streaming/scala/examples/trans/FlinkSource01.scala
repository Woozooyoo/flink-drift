package org.apache.flink.streaming.scala.examples.trans

import org.apache.flink.streaming.api.scala._

object  FlinkSource01 {

  def main(args: Array[String]): Unit = {

    // 1. 创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 2. 获取数据源（Source）
//    val stream = env.readTextFile("test00.txt")
//    val stream = env.socketTextStream("localhost", 11111)
    val list = List(1,2,3,4)

    val iter = Iterator(1,2,3,4)

    val stream = env.generateSequence(1, 20)
//    val stream = env.fromCollection(list)
//    val stream = env.fromCollection(iter)

    // 3. 打印数据（Sink）
    stream.print()

    // 4. 执行任务
    env.execute("FirstJob")
  }

}
