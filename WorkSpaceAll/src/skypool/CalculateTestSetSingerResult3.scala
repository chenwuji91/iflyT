package skypool

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

object CalculateTestSetSingerResult3 {
     def main(args: Array[String]) {
       //      System.setProperty("hadoop.home.dir","D:\\lib\\hadoop")
            val conf = new SparkConf().setAppName("test3")
            //----------------------公司-----------------------
            .setMaster("spark://192.168.86.41:7077")
            .setJars(List("/Users/chenwuji/Workspace/out/artifacts/wj/wj.jar"))
            .set("spark.executor.memory", "10g")//Application Properties。默认1g
            .set("spark.executor.cores", "8")//Execution Behavior。默认全部
            .set("spark.cores.max","80");
            val sc = new SparkContext(conf)
            print("Begin Program");


       val songListenTime = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/skypool1/testing_data/dayBydayFinal/*")
         .map(x => x.split(",|\\)|\\(")).map(x=>(x(1),x(2))).map(x=>x._1).distinct.cache()
       println(songListenTime.count())
//       val Date123 = 20150701;
//       for(Date123 <- 20150715 to 20150730)
//         {
//           val currentDay = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/skypool1/testing_data/dayByday/"+Date123)
//             .map(x => x.split(",|\\)|\\(")).map(x=>(x(1),x(2),x(3).toInt,x(4).toInt,x(5).toInt)).cache()
//           val songCount = currentDay.groupBy(x=>x._2).map(x=>(x._1,x._2.toList.length))
//           deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/skypool1/testing_data/dayBydayFinal/"+Date123);
//           songCount.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/skypool1/testing_data/dayBydayFinal/"+Date123)
//
//         }
//       for(Date123 <- 20150801 to 20150830)
//       {
//         val currentDay = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/skypool1/testing_data/dayByday/"+Date123)
//           .map(x => x.split(",|\\)|\\(")).map(x=>(x(1),x(2),x(3).toInt,x(4).toInt,x(5).toInt)).cache()
//         val songCount = currentDay.groupBy(x=>x._2).map(x=>(x._1,x._2.toList.length))
//         deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/skypool1/testing_data/dayBydayFinal/"+Date123);
//         songCount.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/skypool1/testing_data/dayBydayFinal/"+Date123)
//       }

         sc.stop()
     }
  def deleteHDFSDir(hdfsPathStr: String): Unit = {
           val hdfs = FileSystem.get(new URI("hdfs://192.168.86.41:9000"), new Configuration())
           val outputDir = new Path(hdfsPathStr)
           if (hdfs.exists(outputDir))
             hdfs.delete(outputDir, true)
         }

}