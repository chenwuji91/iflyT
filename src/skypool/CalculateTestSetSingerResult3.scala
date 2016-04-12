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


//       val songListenTime = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/skypool1/testing_data/dayBydayFinal/AllResult")  //读取歌曲次数  前面是歌曲名称  后面接着是次数
//         .map(x => x.split(",|\\)|\\(")).map(x=>(x(1),x(2))).cache()
//       println(songListenTime.count())
       val singerSong = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/skypool").map(x=>x.split(","))   //读取歌手和歌曲之间的映射表
          .map(x=>(x(0),x(1)))

       val Date123 = 0;
       for(Date123 <- 20150716 to 20150731)
       {
         val songListenTime = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/skypool1/testing_data/dayBydayFinal/"+Date123)  //读取歌曲次数  前面是歌曲名称  后面接着是次数
           .map(x => x.split(",|\\)|\\(")).map(x=>(x(1),x(2))).cache()
         println(songListenTime.count())
         val afterjoin = songListenTime.leftOuterJoin(singerSong).map(x => {
           x._2._2 match {
             case Some(singer) => (x._1,x._2._1, singer)  //歌曲 次数 歌唱家
             case None => (x._1, "null", "null")
           }
         }).map(x=>(x._3,x._2.toInt)).cache();
         afterjoin.take(10).foreach(println);

         val resultTime = afterjoin.reduceByKey((x,y)=>x+y);
         resultTime.take(10).foreach(println);

         deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/skypool1/TestResult/"+Date123);
         resultTime.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/skypool1/TestResult/"+Date123)

       }
       for(Date123 <- 20150801 to 20150830)
       {
         val songListenTime = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/skypool1/testing_data/dayBydayFinal/"+Date123)  //读取歌曲次数  前面是歌曲名称  后面接着是次数
           .map(x => x.split(",|\\)|\\(")).map(x=>(x(1),x(2))).cache()
         println(songListenTime.count())
         val afterjoin = songListenTime.leftOuterJoin(singerSong).map(x => {
           x._2._2 match {
             case Some(singer) => (x._1,x._2._1, singer)  //歌曲 次数 歌唱家
             case None => (x._1, "null", "null")
           }
         }).map(x=>(x._3,x._2.toInt)).cache();
         afterjoin.take(10).foreach(println);

         val resultTime = afterjoin.reduceByKey((x,y)=>x+y);
         resultTime.take(10).foreach(println);

         deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/skypool1/TestResult/"+Date123);
         resultTime.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/skypool1/TestResult/"+Date123)
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