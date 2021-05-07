package com.ztwu.bigdata.demo.getSideOutput

import org.apache.flink.streaming.api.scala._
import scala.collection.mutable.ArrayBuffer

object MyRandom {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置线程数为1
    env.setParallelism(1)
    //设置数据源为1到100的集合
    val arr = ArrayBuffer[Int]()
    for(i <- 1 to 100){
      arr.append(i)
    }
    val ds = env.fromCollection(arr)

    //把数据源按指定规则split为奇数流和偶数流
    val res = ds.split(num => {
      if (num % 2 == 0) Seq("even")
      else Seq("odd")
    })

    val even = res.select("even")
    val odd = res.select("odd")

    //打印输出奇数流和偶数流
    even.print("even")
    odd.print("odd")
    //执行环境触发执行
    env.execute()

  }
}
