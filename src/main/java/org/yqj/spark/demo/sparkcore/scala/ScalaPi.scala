package org.yqj.spark.demo.sparkcore.scala

import org.apache.spark.util.random
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yaoqijun.
  * Date:2016-07-15
  * Email:yaoqj@terminus.io
  * Descirbe:
  */
object ScalaPi {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("scalaMainTest")
    val spark = new SparkContext(conf)
    val count = spark.parallelize(1 unit 500000, 2).map{ i=>
      val x = scala.math.random * 2 - 1
      val y = scala.math.random * 2 - 1
      if (x * x + y * y < 1) 1 else 0
    }.reduce(_+_)
    println("Pi calculate result is : " + 4 * count / 500000 )
    spark.stop()
  }
}
