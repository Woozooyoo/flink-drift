package org.apache.flink.streaming.scala.examples.trans

import org.apache.flink.streaming.api.scala._

object Transformation01 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    /* 1. Map操作 */
    val stream1 = env.readTextFile("./flink-api-example/src/main/resources/test00.txt")
    val streamMap = stream1.map(item => item.split(" "))  //split后，是string数组
//    streamMap.print()

    /*2. FlatMap操作*/
    val stream2 = env.readTextFile("./flink-api-example/src/main/resources/test00.txt")
    val streamFlat = stream2.flatMap(item => item.split(" ")) //split后，是string数组，flatmap把数组也打散
//    streamFlat.print()  //一行一个线程

    /*3. Filter操作 */
    val stream3 = env.generateSequence(1, 10)
    val streamFilter = stream3.filter(item => item != 1)
//    streamFilter.print()

    /*4. Connect + CoMap/CoFlatMap 操作!!!!!!!!!!!!!!
    * 只是放在同一个流中，内部各自相互独立*/
    val stream41 = env.generateSequence(1,10)
    val stream42 = env.readTextFile("./flink-api-example/src/main/resources/test00.txt")
      .flatMap(item => item.split(" "))

    val streamConnect: ConnectedStreams[Long, String] = stream41.connect(stream42)

    val streamCoMap = streamConnect.map(item => item * 2, item => (item, 1L))
//    streamCoMap.print()


    /*5. Split + Select 操作*/
    val stream = env.readTextFile("./flink-api-example/src/main/resources/test00.txt")
      .flatMap(item => item.split(" "))

    val streamSplit = stream.split(
      word =>
        ("wisdom".equals(word)) match {
          case true => List("wisdom")
          case false => List("foolishness")
        }
    )

    val streamSelect01 = streamSplit.select("wisdom")
    val streamSelect02 = streamSplit.select("foolishness")
//    streamSelect01.print()
//    streamSelect02.print()

    /*6. Union 操作*/
    val stream01 = env.readTextFile("./flink-api-example/src/main/resources/test00.txt").flatMap(item => item.split(" "))
    val stream02 = env.readTextFile("./flink-api-example/src/main/resources/test01.txt").flatMap(item => item.split(" "))

    val streamUnion = stream01.union(stream02)

    streamUnion.print()

    env.execute("FirstJob")
  }

}
