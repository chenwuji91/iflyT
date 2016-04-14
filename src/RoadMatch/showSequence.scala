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
            val conf = new SparkConf().setAppName("wj")
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
       val sequence1 = genContMovingTripleSeq(sc,"hdfs://192.168.86.41:9000/user/wjchen/asdOut04143.txt")
//       val szf = sequence1.map()collect()
       val sequence2 = sequence1.map(x=>x._2).flatMap(x=>x).flatMap(x=>x.toSeq).cache();



       val seqwithoutTime = sequence2.map(x=>(x._2.substring(0,8),x._1)).groupByKey().cache();
       println(seqwithoutTime.count())
//       onePoint(sc,sequence1)
      // calculatePairFrequency(sc,sequence1)
       seqwithoutTime.take(100).foreach(println)
       seqwithoutTime.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/asdOut04144WithNoDate")
       val seqwithTime = sequence2.map(x=>(x._2.substring(0,8),(x._1,x._2))).groupByKey().cache();
       seqwithTime.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/asdOut04144WithDate")
       sc.stop()
     }





  def deleteHDFSDir(hdfsPathStr: String): Unit = {
           val hdfs = FileSystem.get(new URI("hdfs://192.168.86.41:9000"), new Configuration())
           val outputDir = new Path(hdfsPathStr)
           if (hdfs.exists(outputDir))
             hdfs.delete(outputDir, true)
         }

}