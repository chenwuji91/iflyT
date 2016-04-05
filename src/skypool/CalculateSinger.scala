package skypool

import java.io.{File, PrintWriter}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

object CalculateSinger {
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


       val DataRdd = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/skypool") //歌曲和用户的数据
           .map(x => x.split(",")).map(x=>(x(0),x(1),x(2).toInt,x(3).toInt,x(4).toInt,x(5).toInt)).cache()
       val userAction = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/skyPoolUserAction")
           .map(x => x.split(",")).map(x=>(x(0),x(1),x(2).toInt,x(3).toInt,x(4).toInt)).cache()

       val userCount = userAction.map(x=>x._1).distinct().count();
        print("User Count: "+userCount);
 //      val newFaXing = DataRdd.filter(x=>x._3>20160000).cache();

//       val yirenList = DataRdd.map(x=>x._2).distinct().cache();

  //     println("艺人数量"+yirenList.count());
//
//      val singer = DataRdd.map(x=>x._1).distinct()
//       println("歌曲列表"+ singer.count());
//    singer.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/skypool1/SongList")
//       newFaXing.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/skypool1/3-8Month")
//       yirenList.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/skypool1/yirenList")




//
//        val writer = new PrintWriter(new File("/Users/chenwuji/Documents/output/deleteRatio"+Date1+".txt"))
//         writer.println("+******************>两小时的有"+stayMoreThan2Hour.count())
//         writer.println("+******************<两小时的有"+stayLessThan2Hour.count())
//
//         writer.close()

         sc.stop()
     }
  def deleteHDFSDir(hdfsPathStr: String): Unit = {
           val hdfs = FileSystem.get(new URI("hdfs://192.168.86.41:9000"), new Configuration())
           val outputDir = new Path(hdfsPathStr)
           if (hdfs.exists(outputDir))
             hdfs.delete(outputDir, true)
         }
}