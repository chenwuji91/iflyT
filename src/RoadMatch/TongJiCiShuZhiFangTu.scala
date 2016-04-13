//package RoadMatch
//
//import java.net.URI
//
//import com.iflytek.wuhu.ContMovingSeqUtil._
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}
//
//
///**
//  * 本程序主要是搜索移动序列中的频繁项集
//  */
//object TongJiCiShuZhiFangTu {
//
//     def main(args: Array[String]) {
//            val conf = new SparkConf().setAppName("wjchen")
//            //----------------------公司-----------------------
//            .setMaster("spark://192.168.86.41:7077")
//            .setJars(List("/Users/chenwuji/Workspace/out/artifacts/wj/wj.jar", "/Users/chenwuji/Documents/lib/spark/iflytek-sequence-0.1.1.jar"))
//            .set("spark.executor.memory", "10g")//Application Properties。默认1g
//            .set("spark.executor.cores", "8")//Execution Behavior。默认全部
//            .set("spark.cores.max","80");
//            val sc = new SparkContext(conf)
//            print("Begin Program");
/////user/wjchen/countTill0413/*
//       val times = genContMovingCISeq(sc,"hdfs://192.168.86.41:9000/user/wjchen/countTill0413/*").cache()
//
//       sc.stop()
//     }
//
//
//
//
//
//  def deleteHDFSDir(hdfsPathStr: String): Unit = {
//           val hdfs = FileSystem.get(new URI("hdfs://192.168.86.41:9000"), new Configuration())
//           val outputDir = new Path(hdfsPathStr)
//           if (hdfs.exists(outputDir))
//             hdfs.delete(outputDir, true)
//         }
//
//}