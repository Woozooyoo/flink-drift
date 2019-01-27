package org.apache.flink.streaming.scala.examples.trans

import org.apache.flink.streaming.api.scala._

object Transformation02 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // need window to solve reduce by time problem
    val stream: DataStream[(String, Long)] = env.readTextFile("./flink-api-example/src/main/resources/test00.txt")
      .flatMap(item => item.split(" "))
      .map(item => (item, 1L))
      .keyBy(0)
      .reduce( (item1, item2) => (item1._1, item1._2 + item2._2) )
//      .sum(1)

    stream.print()

    env.execute("KeyJob")
  }
}
