/**
 * Created by liuti on 2016/8/4.
 */

import  org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
object SimpleApp {
  def main(args: Array[String]) {
    val logfile="file:///usr/local/module/practise/sogouquery/data/SogouQ.reduced";
    val conf=new SparkConf();
    conf.setAppName("Simple Application")
    val sc=new SparkContext(conf)
    val logData=sc.textFile(logfile,2).cache()
    val numAs=logData.filter(line=>line.contains("zhidao.baidu.com")).count()
    val numBs=logData.filter(line=>line.contains("www.ehtxz.cn")).count()
    println("Line with a:%s,Lines with b: %s".format(numAs,numBs))
  }
}
