package org.apache.flink.streaming.scala.examples.windowing

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object Window01 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text: DataStream[String] = env.socketTextStream("localhost", 9999)

    val counts: DataStream[(String, Int)] = text
      // split up the lines in pairs (2-tuple) containing: (word,1)
      .flatMap(_.toLowerCase.split("\\W+")) //[^A-Za-z0-9_]
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      // create windows of windowSize records slided every slideSize records
//      .countWindow(5) // every single key appear 5 then execute, not total 5
//      .countWindow(5,2) // 出现2次 key 统计最近的5次key
//      .timeWindow(Time.seconds(5)) // every 5 seconds then execute
      .timeWindow(Time.seconds(5),Time.seconds(2)) // every 2 seconds then execute, count the previous 5 seconds
      // group by the tuple field "0" and sum up tuple field "1"
      .sum(1)

    counts.print().setParallelism(2)

    env.execute("WindowJob")
  }
}
