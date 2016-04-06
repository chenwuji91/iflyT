package RoadMatch

import java.net.URI
import java.io.PrintWriter
import java.net.URL
import com.iflytek.wuhu.ContMovingSeqUtil
import com.iflytek.wuhu.ContMovingSeqUtil._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import java.io.PrintWriter
import java.io._
import scala.io.Source
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import java.net.URI
import com.databricks.spark._


/**
  * 本程序主要是搜索移动序列中的频繁项集
  */
object FrequencySequence {

     def main(args: Array[String]) {
       //      System.setProperty("hadoop.home.dir","D:\\lib\\hadoop")
            val conf = new SparkConf().setAppName("wjchen")
            //----------------------公司-----------------------
            .setMaster("spark://192.168.86.41:7077")
            .setJars(List("/Users/chenwuji/Workspace/out/artifacts/wj/wj.jar", "/Users/chenwuji/Documents/lib/spark/iflytek-sequence-0.1.1.jar"))
            .set("spark.executor.memory", "10g")//Application Properties。默认1g
            .set("spark.executor.cores", "8")//Execution Behavior。默认全部
            .set("spark.cores.max","80");
            val sc = new SparkContext(conf)
            print("Begin Program");
         //val seqRdd = genContMovingTripleSeq(sc,"hdfs://192.168.86.41:9000/user/ibs/result/20160310/*")
         // seqRdd.take(100).foreach(println)
       val sequence1 = genContMovingCISeq(sc,"hdfs://192.168.86.41:9000/user/ibs/result/20160310/*")


       sequence1.take(100).foreach(println)
         sc.stop()
     }
  def onePoint(sc:SparkContext,rDD: RDD[(String, List[List[String]])]):RDD[(String,String)] = {
      val onePoint = rDD.mapValues()
  }
  def deleteHDFSDir(hdfsPathStr: String): Unit = {
           val hdfs = FileSystem.get(new URI("hdfs://192.168.86.41:9000"), new Configuration())
           val outputDir = new Path(hdfsPathStr)
           if (hdfs.exists(outputDir))
             hdfs.delete(outputDir, true)
         }

}