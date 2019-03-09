package org.apache.flink.streaming.scala.examples.windowing

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

object Window01 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text: DataStream[String] = env.socketTextStream("192.168.1.102", 9999)

    val counts: DataStream[(String, Int)] = text
      // split up the lines in pairs (2-tuple) containing: (word,1)
      .flatMap(_.toLowerCase.split("\\W+")) //[^A-Za-z0-9_]
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      // create windows of windowSize records slided every slideSize records
//      .countWindow(5) // every single key appear 5 then execute, not total 5 单个key达到5条，a b c d e不执行，a a a a a 执行
//      .countWindow(5,2) // 出现2次 key 统计最近的5次key a a b a a
      .timeWindow(Time.seconds(5)) // every 5 seconds then execute
//      .timeWindow(Time.seconds(5),Time.seconds(2)) // every 2 seconds then execute, count the previous 5 seconds
//      .window(EventTimeSessionWindows.withGap(Time.minutes(5))) //the time between previous et and this et >=5 execute

      //.reduce/fold/
//      .apply() //聚合分类  被弃用，用process全量(等窗格的数据到齐才开始 适合:求九分位、排序等全部到齐才能进行的运算)
//      .process()//聚合分类  全量(等窗格的数据到齐才开始 适合:求九分位、排序等全部到齐才能进行的运算)
      // group by the tuple field "0" and sum up tuple field "1"
      .sum(1) // 增量(每来一条数据更新window中的数据)

    counts.print().setParallelism(2)

    env.execute("WindowJob")
  }
}
