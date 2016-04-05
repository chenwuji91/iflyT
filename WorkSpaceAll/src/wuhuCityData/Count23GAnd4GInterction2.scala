package wuhuCityData

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 本程序的主要目的是进行春节期间在芜湖停留天数的判断
  * 程序与judgeTravelOrLocal的程序类似  读取的数据源都是原始数据
  * 只是本程序最后保存的数据格式不是该用户出现的最早时间和最晚时间的时间差,而是保存的是在这段时间内的出现天数
  */
object Count23GAnd4GInterction2 {
  def main(args: Array[String]) {
    //      System.setProperty("hadoop.home.dir","D:\\lib\\hadoop")
    val conf = new SparkConf().setAppName("wjchen")
      //----------------------公司-----------------------
      .setMaster("spark://192.168.86.41:7077")
      .setJars(List("/Users/chenwuji/Workspace/out/artifacts/wj/wj.jar"))
      .set("spark.executor.memory", "10g")//Application Properties。默认1g
      .set("spark.executor.cores", "8")//Execution Behavior。默认全部
      .set("spark.cores.max","80");
    val sc = new SparkContext(conf)
    print("Begin Program");

    val g23 = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/23GEMIC.txt")
        .map(x => x.split("	")).map(x => x(1)).cache()
    val g4 = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/g4EMIC2")
    val map4g = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/4GIMSI_23GIMSI_MAP.txt")
      .map(x => x.split("\t")).map(x => x(0)).cache()
    val map23g = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/4GIMSI_23GIMSI_MAP.txt")
      .map(x => x.split("\t")).map(x => x(1)).cache()


    println(g4.count())

      println("******************23GYOU "+g23.intersection(map23g).distinct().count())
      println("+******************4G有"+g4.intersection(map4g).count())

    println("******************23G33YOU "+g23.intersection(map23g).distinct().count())
    println("+******************4G33有"+g4.intersection(map4g).count())


    sc.stop()
  }
  def deleteHDFSDir(hdfsPathStr: String): Unit = {
    val hdfs = FileSystem.get(new URI("hdfs://192.168.86.41:9000"), new Configuration())
    val outputDir = new Path(hdfsPathStr)
    if (hdfs.exists(outputDir))
      hdfs.delete(outputDir, true)
  }
}