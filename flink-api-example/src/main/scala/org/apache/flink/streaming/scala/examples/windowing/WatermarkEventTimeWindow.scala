package org.apache.flink.streaming.scala.examples.windowing

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object WatermarkEventTimeWindow {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    /** 设置 eventTime*/
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //Watermarks解决乱序配合window 延迟触发机制，设置一个延时时长
    val text: DataStream[String] = env
      .socketTextStream("localhost", 9999)  //nc -l -p  nc -lk
      // 对每一条数据都调用这个方法，吧eventTime提取出来
      .assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(3)) {// (eventTime-3000ms)的时间是否大于下面window的5秒、10秒、15秒
        override def extractTimestamp(element: String): Long = {
          // EventTime是日志生成时间，我们从日志中解析EventTime，空格分隔第一个元素是时间
          val eventTime = element.split(" ")(0).toLong
          println(eventTime)
          // milliseconds
          eventTime
        }
      })

    val counts: DataStream[(String, Int)] = text
      //空格分隔第二个元素是word           1000,foolishness
      .map(item=> (item.split(" ")(1), 1))
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(5))) //滚动窗口 5秒一个 5秒一个 窗口之间不会重叠 //BI统计，每个时间段的聚合计算
//      .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))) //包含前面的10秒数据， (滑动窗口，可能会重叠，窗口大小和步长) (33 a)=>(43,a)会把(38)的也计算// 例如根据最近5min的失败率决定是否报警
//      .window(EventTimeSessionWindows.withGap(Time.seconds(5))) //the time between previous et and this et >=5 execute
      //会话窗口(time)，相邻两条数据的时间超过5秒，就把前面的数据组成窗口  //适用于线上用户行为分析
      .sum(1)

    counts.print().setParallelism(2)

    env.execute("WindowJob")
  }
}

/**
  * 并发角度的WaterMark  shuffle后会取最小的WaterMark*/
