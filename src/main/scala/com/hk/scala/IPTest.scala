package com.hk.scala

object IPTest {
  //将IP转十进制
  def ipToLong(ip: String): Long = {
    //将IP地址按照"."进行切
    val fragments: Array[String] = ip.split("[.]")
    var ipNum: Long = 0L
    //位移运算:左移
    for (i <- fragments.indices) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def main(args: Array[String]): Unit = {
    val s = "127.0.0.1"
    val re: Long = ipToLong(s)
    println(re)
  }
}
