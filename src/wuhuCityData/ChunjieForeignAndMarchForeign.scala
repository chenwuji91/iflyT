

package wuhuCityData

//import myjava.AESSecurityUtil
//import myjava.AESSecurityUtil._
import java.io.{File, PrintWriter}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计过年游客  平时游客的情况   过年本地人  平时外地人  过年本地人 过年外地人  平时本地人  求解
  * 不涉及到归属地的相关信息  所以只是对没有解密的数据集进行相关的操作
  */
object ChunjieForeignAndMarchForeign {
   def main(args: Array[String]) {
 //           System.setProperty("hadoop.home.dir","/Users/chenwuji/Documents/lib/hadoop/")
            val conf = new SparkConf().setAppName("test456")
            //----------------------公司-----------------------
            .setMaster("spark://192.168.86.41:7077")
            .setJars(List("/Users/chenwuji/Workspace/out/artifacts/wj/wj.jar"))
            .set("spark.executor.memory", "10g")//Application Properties。默认1g
            .set("spark.executor.cores", "8")//Execution Behavior。默认全部
            .set("spark.cores.max","80")
            val sc = new SparkContext(conf)
            print("Begin")

//    val RefToFTGetTravellerRdd = sc.textFile("hdfs://192.168.86.41:9000/user/ekyang/MyFormatWUHUSeq/RefToFTTraveller/*")
        val HdfsFilePath = "hdfs://192.168.86.41:9000//user/wjchen/14DAY/*"
        val rdd = sc.textFile(HdfsFilePath).map(x => x.split(",|\\)|\\(")).map(x => (x(1),x(2).toInt)).map(x => x._1).distinct().cache()//本地人rdd



     val PingshiRdd = sc.textFile("hdfs://192.168.86.41:9000//user/wjchen/March/*")
               .map(x => x.split(",|\\)|\\(")).map(x => (x(1),x(2).toInt)).map(x => x._1).distinct().cache()//平时旅游RDD

     val GuonianRdd = sc.textFile("hdfs://192.168.86.41:9000//user/wjchen/11DAY/*")
       .map(x => x.split(",|\\)|\\(")).map(x => (x(1),x(2).toInt)).map(x => x._1).distinct().cache()//过年旅游RDD


     val writer = new PrintWriter(new File("/Users/chenwuji/Documents/output/PingshiChunjieBenDIWaiDi.txt"))


     val rdd1 = GuonianRdd.subtract(rdd)    //春节在芜湖的减去本地人,就是外地旅游的人,流入人口
      writer.println("过年外地的人     "+rdd1.count())

     val rdd2 = PingshiRdd.subtract(rdd)    //春节在芜湖的减去本地人,就是外地旅游的人,流入人口
     writer.println("平时外地的人     "+rdd2.count())

     val rdd3 = GuonianRdd.intersection(rdd);
     writer.println("过年本地的人     "+rdd3.count())

     val rdd4 = PingshiRdd.intersection(rdd);
     writer.println("平时本地的人     "+rdd4.count())


     writer.close();
            sc.stop()
            }


  def deleteHDFSDir(hdfsPathStr: String): Unit = {
    val hdfs = FileSystem.get(new URI("hdfs://192.168.86.41:9000"), new Configuration())
    val outputDir = new Path(hdfsPathStr)
    if (hdfs.exists(outputDir))
      hdfs.delete(outputDir, true)
  }
}