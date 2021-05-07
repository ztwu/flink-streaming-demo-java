package com.ztwu.bigdata.demo.getSideOutput

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import scala.collection.mutable.ArrayBuffer

object MyRandom2 {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置线程数
    env.setParallelism(8)
    //设置数据源为1到100的集合
    val arr = ArrayBuffer[Int]()
    for(i <- 1 to 100){
      arr.append(i)
    }
    val ds = env.fromCollection(arr)

    //用process方法实现分流
    val res = ds.process(new EO())
    val odd = res.getSideOutput(new OutputTag[String]("otest"))
    val even = res.getSideOutput(new OutputTag[String]("etest"))

    even.print()
    odd.print()

    env.execute()

  }
}


class EO()extends ProcessFunction[Int,Int]{
  lazy val otag = new OutputTag[String]("otest")
  lazy val etag = new OutputTag[String]("etest")
  //重写processElement方法指定分流规则
  override def processElement(i: Int, context: ProcessFunction[Int, Int]#Context, collector: Collector[Int]): Unit = {
    if (i%2==0) context.output(etag,"even"+i)
    else context.output(otag,"odd"+i)
  }
}
