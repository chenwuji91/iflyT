package RoadMatch

import java.net.URI

import com.iflytek.wuhu.ContMovingSeqUtil._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 本程序主要是搜索移动序列中的频繁项集
  */
object showSequence {

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
         //val seqRdd = genContMovingTripleSeq(sc,"hdfs://192.168.86.41:9000/user/ibs/result/20160310/*")"hdfs://192.168.86.41:9000/user/wjchen/roadtestResult1.txt"
         // seqRdd.take(100).foreach(println)
       val sequence1 = genContMovingTripleSeq(sc,"hdfs://192.168.86.41:9000/user/wjchen/szf0411.txt")
       sequence1.take(100).foreach(println)
//       onePoint(sc,sequence1)
      // calculatePairFrequency(sc,sequence1)
       sc.stop()
     }
//
//  def calculatePairFrequency(sc:SparkContext,rDD: RDD[(String, List[List[String]])]):RDD[(Any, Int)] = {
//    val pair:RDD[List[Any]] = pairTriGen(sc,rDD)
//    val allpair = pair.flatMap(x=>x.toList).map(x=>(x,1))
//    val pairCount = allpair.reduceByKey(_+_)
//    deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/test12345")
//    pairCount.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/test12345")
//    pairCount
//
//  }
//
//  def pairTriGen(sc:SparkContext,rDD: RDD[(String, List[List[String]])]):RDD[List[Any]] = {
//    //var list:List[String] = List("")
//    val onePoint = rDD.map(x => x._2)
//    val oneSeq = onePoint.flatMap(x => x.toList)
//    val pair = oneSeq.map(x=>{
//      var l: List[Any] = List()
//      if(x.length>2)
//      {
//        for(i:Int<- 0 to x.length-3)
//        {
//          val temp:List[(String, String,String)] = List((x(i),x(i+1),x(i+2)))
//          l = l:::temp
//        }
//      }
//      l
//    })
//
//    pair
//  }
//
//  def pairGen(sc:SparkContext,rDD: RDD[(String, List[List[String]])]):RDD[List[Any]] = {
//    //var list:List[String] = List("")
//    val onePoint = rDD.map(x => x._2)
//    val oneSeq = onePoint.flatMap(x => x.toList)
//    val pair = oneSeq.map(x=>{
//      var l: List[Any] = List()
//      if(x.length>1)
//        {
//          for(i:Int<- 0 to x.length-2)
//          {
//            val temp:List[(String, String)] = List((x(i),x(i+1)))
//            l = l:::temp
//          }
//        }
//       l
//    })
//
//    pair
//  }
//
//
//  def onePoint(sc:SparkContext,rDD: RDD[(String, List[List[String]])]):RDD[(String, Int)] = {
//    //var list:List[String] = List("")
//    val onePoint = rDD.map(x=>x._2)
//    val newList = onePoint.flatMap(x => x.toList )
//    val countOne = newList.flatMap(x => x.toList)
//    val times = countOne.map(x=>((x),1)).reduceByKey(_+_)
//    deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/test123")
//    times.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/test123")
//
//    return times
//  }




  def deleteHDFSDir(hdfsPathStr: String): Unit = {
           val hdfs = FileSystem.get(new URI("hdfs://192.168.86.41:9000"), new Configuration())
           val outputDir = new Path(hdfsPathStr)
           if (hdfs.exists(outputDir))
             hdfs.delete(outputDir, true)
         }

}