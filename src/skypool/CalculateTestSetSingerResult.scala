package skypool

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

object CalculateTestSetSingerResult {
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


       val testData1 = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/user_actionHotCold.csv")
          .map(x => x.split(","))//.map(x=>(x(1),x(2),x(3).toInt,x(4).toInt,x(5).toInt)).cache()
       val illegal = testData1.filter(x=>x.length<5)
       print("Illegal set")
       illegal.take(1000).foreach(println)
       val testData = testData1.filter(x=>x.length>=5).map(x=>(x(1),x(2),x(3).toInt,x(4).toInt,x(5).toInt)).cache()
       val Date123 = 0;
       for(Date123 <- 20150701 to 20150731)
       {
         val oneDay = testData.filter(x=>x._5==Date123)
         deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/tianchiProtoByDay"+Date123);
         oneDay.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/tianchiProtoByDay"+Date123);
       }

       for(Date123 <- 20150801 to 20150830)
       {
         val oneDay = testData.filter(x=>x._5==Date123)
         deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/tianchiProtoByDay"+Date123);
         oneDay.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/tianchiProtoByDay"+Date123);
       }
       for(Date123 <- 20150601 to 20150630)
       {
         val oneDay = testData.filter(x=>x._5==Date123)
         deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/tianchiProtoByDay"+Date123);
         oneDay.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/tianchiProtoByDay"+Date123);
       }

       for(Date123 <- 20150501 to 20150531)
       {
         val oneDay = testData.filter(x=>x._5==Date123)
         deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/tianchiProtoByDay"+Date123);
         oneDay.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/tianchiProtoByDay"+Date123);
       }
       for(Date123 <- 20150401 to 20150430)
       {
         val oneDay = testData.filter(x=>x._5==Date123)
         deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/tianchiProtoByDay"+Date123);
         oneDay.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/tianchiProtoByDay"+Date123);
       }

       for(Date123 <- 20150301 to 20150331)
       {
         val oneDay = testData.filter(x=>x._5==Date123)
         deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/tianchiProtoByDay"+Date123);
         oneDay.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/tianchiProtoByDay"+Date123);
       }



         sc.stop()
     }
  def deleteHDFSDir(hdfsPathStr: String): Unit = {
           val hdfs = FileSystem.get(new URI("hdfs://192.168.86.41:9000"), new Configuration())
           val outputDir = new Path(hdfsPathStr)
           if (hdfs.exists(outputDir))
             hdfs.delete(outputDir, true)
         }

}