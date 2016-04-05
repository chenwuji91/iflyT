package skypool

import java.io.{File, PrintWriter}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.catalyst.expressions.UnixTimestamp
import org.apache.spark.{SparkConf, SparkContext}

object SeparateTestSet {
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

//       val userAction = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/skyPoolUserAction")
//           .map(x => x.split(",")).map(x=>(x(0),x(1),x(2).toInt,x(3).toInt,x(4).toInt)).cache()

       val userAction3_715 = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/skyPoolUserAction")
         .map(x => x.split(",")).map(x=>(x(0),x(1),TransUnixToDateTime.translate(x(2).toInt).toInt,x(3).toInt,x(4).toInt)).filter(x=>x._3>2015030000).filter(x=>x._3<2015071600).cache()
       val userAction715_830 = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/skyPoolUserAction")
         .map(x => x.split(",")).map(x=>(x(0),x(1),TransUnixToDateTime.translate(x(2).toInt).toInt,x(3).toInt,x(4).toInt)).filter(x=>x._3>2015071500).filter(x=>x._3<2015083200).cache()

      deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/skypool1/training_data_new")
       deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/skypool1/testing_data_new")
       userAction3_715.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/skypool1/training_data_new");
       userAction715_830.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/skypool1/testing_data_new");
         sc.stop()
     }
  def deleteHDFSDir(hdfsPathStr: String): Unit = {
           val hdfs = FileSystem.get(new URI("hdfs://192.168.86.41:9000"), new Configuration())
           val outputDir = new Path(hdfsPathStr)
           if (hdfs.exists(outputDir))
             hdfs.delete(outputDir, true)
         }
//  def translate(uTime: Int):Unit = {
//          val time:Int = uTime / 86400 + ( 3600 * 8 )/ 86400 + 25569
//  }
}