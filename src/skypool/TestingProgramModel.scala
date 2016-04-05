package skypool

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

object TestingProgramModel {
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



       val userActionTest = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/skyPoolUserAction")
         .map(x => x.split(",")).map(x=>(x(0),x(1),x(2).toInt,x(3).toInt,x(4).toInt)).filter(x=>x._5>20150715).filter(x=>x._5<20150832).cache()

       val song = userActionTest.groupBy(_._2).map(x=>(x._1,x._2.toList.size))

       deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/skypool1/songListenTime78")
       song.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/skypool1/songListenTime78");
         sc.stop()
     }
  def deleteHDFSDir(hdfsPathStr: String): Unit = {
           val hdfs = FileSystem.get(new URI("hdfs://192.168.86.41:9000"), new Configuration())
           val outputDir = new Path(hdfsPathStr)
           if (hdfs.exists(outputDir))
             hdfs.delete(outputDir, true)
         }
}