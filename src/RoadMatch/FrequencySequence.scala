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
       val sequence1 = genContMovingCISeq(sc,"hdfs://192.168.86.41:9000/user/ibs/result/*/*")
 //      sequence1.take(10).foreach(println)
//       onePoint(sc,sequence1)
       val countResult = calculatePairFrequency(sc,sequence1)//完成次数的统计
       zhifangtu(sc,countResult)
       sc.stop()
     }
//统计一下各个次数 为画直方图做一些服务
def zhifangtu(sc:SparkContext, countResult:RDD[(Any, Int)]):RDD[(Int,Int)] = {
  val timeRdd = countResult.map(x=>(x._2,x._1)).groupByKey().map(x=>(x._1,x._2.toList.length)).cache()
  deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/countTill0413ZhiFangTu")
  timeRdd.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/countTill0413ZhiFangTu")
  timeRdd

}

//统计一下序列的频繁项
  def calculatePairFrequency(sc:SparkContext,rDD: RDD[(String, List[List[String]])]):RDD[(Any, Int)] = {
    val pair:RDD[List[Any]] = pairTriGen(sc,rDD)
    val allpair = pair.flatMap(x=>x.toList).map(x=>(x,1))
    val pairCount = allpair.reduceByKey(_+_).cache()
    deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/countTill0413")
    pairCount.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/countTill0413")

    pairCount

  }


  //统计的是三个序列的频繁项
  def pairTriGen(sc:SparkContext,rDD: RDD[(String, List[List[String]])]):RDD[List[Any]] = {
    //var list:List[String] = List("")
    val onePoint = rDD.map(x => x._2)
    val oneSeq = onePoint.flatMap(x => x.toList)
    val pair = oneSeq.map(x=>{
      var l: List[Any] = List()
      if(x.length>2)
      {
        for(i:Int<- 0 to x.length-3)
        {
          val temp:List[(String, String,String)] = List((x(i),x(i+1),x(i+2)))
          l = l:::temp
        }
      }
      l
    })

    pair
  }

  def pairGen(sc:SparkContext,rDD: RDD[(String, List[List[String]])]):RDD[List[Any]] = {
    //var list:List[String] = List("")
    val onePoint = rDD.map(x => x._2)
    val oneSeq = onePoint.flatMap(x => x.toList)
    val pair = oneSeq.map(x=>{
      var l: List[Any] = List()
      if(x.length>1)
        {
          for(i:Int<- 0 to x.length-2)
          {
            val temp:List[(String, String)] = List((x(i),x(i+1)))
            l = l:::temp
          }
        }
       l
    })

    pair
  }


  def onePoint(sc:SparkContext,rDD: RDD[(String, List[List[String]])]):RDD[(String, Int)] = {
    //var list:List[String] = List("")
    val onePoint = rDD.map(x=>x._2)
    val newList = onePoint.flatMap(x => x.toList )
    val countOne = newList.flatMap(x => x.toList)
    val times = countOne.map(x=>((x),1)).reduceByKey(_+_)
    deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/test123")
    times.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/test123")

    return times
  }




  def deleteHDFSDir(hdfsPathStr: String): Unit = {
           val hdfs = FileSystem.get(new URI("hdfs://192.168.86.41:9000"), new Configuration())
           val outputDir = new Path(hdfsPathStr)
           if (hdfs.exists(outputDir))
             hdfs.delete(outputDir, true)
         }

}