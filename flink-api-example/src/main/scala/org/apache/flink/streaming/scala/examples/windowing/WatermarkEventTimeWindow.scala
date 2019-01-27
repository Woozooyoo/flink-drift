package org.apache.flink.streaming.scala.examples.windowing

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object WatermarkEventTimeWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val text: DataStream[String] = env
      .socketTextStream("localhost", 9999)
      // 对每一条数据都调用这个方法，吧eventTime提取出来
      .assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[String](Time.milliseconds(3000)) {// (eventTime-3000ms)的时间是否大于下面window的5秒
        override def extractTimestamp(element: String): Long = {
          // EventTime是日志生成时间，我们从日志中解析EventTime，空格分隔第一个元素是时间
          val eventTime = element.split(" ")(0).toLong
          println(eventTime)
          eventTime
        }
      })

    val counts: DataStream[(String, Int)] = text
      //空格分隔第二个元素是word           1000,foolishness
      .map(item=> (item.split(" ")(1), 1))
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//      .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))) //包含前面的10秒数据，
//      .window(EventTimeSessionWindows.withGap(Time.seconds(5))) //the time between previous et and this et >=5 execute
      .sum(1)

    counts.print().setParallelism(2)

    env.execute("WindowJob")
  }
}
