package wuhuCityData

import java.net.URI

import java.io.{PrintWriter, PrintStream}
import java.net.URI
import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * 本程序主要是接着上面的一个程序  读取带有天数的相关记录文件  通过取差集的形式找到 结合已经分类得到的常住人口的信息  得到在这里的过来旅游的外地人的停留天数
  */
object CountStayDays2 {

     def main(args: Array[String]) {
       //      System.setProperty("hadoop.home.dir","D:\\lib\\hadoop")
            val conf = new SparkConf().setAppName("wjchen")
            //----------------------公司-----------------------
            .setMaster("spark://192.168.86.41:7077")
            .setJars(List("/Users/chenwuji/Workspace/out/artifacts/wj/wj.jar"))
            .set("spark.executor.memory", "10g")//Application Properties。默认1g
            .set("spark.executor.cores", "8")//Execution Behavior。默认全部
            .set("spark.cores.max","80");
            val sc = new SparkContext(conf)
            print("Begin Program");


       val foreignTraveller = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/11DAYContainsDate/*")
         .map(x => x.split(",|\\)|\\(")).map(x => (x(1),x(2).toInt)).cache()//春节旅游RDD

       foreignTraveller.take(10).foreach(println)

         val grouped = foreignTraveller
           .groupByKey().cache()
           .map(t => (t._1, (t._2.max - t._2.min)+1))

       val HdfsFilePath = "hdfs://192.168.86.41:9000//user/wjchen/14DAY/*"
       val rddLocalPeople = sc.textFile(HdfsFilePath).map(x => x.split(",|\\)|\\(")).map(x => (x(1),x(2).toInt)).map(x => x._1).distinct().cache()//本地人rdd

       val calculateLocal = grouped.subtractByKey(rddLocalPeople.map(x=>(x,"null")));
       println("SSSSSSSSSSSS")
       calculateLocal.take(20).foreach(println);
       deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/stayDays1")
       calculateLocal.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/stayDays1")


         sc.stop()
     }
  def deleteHDFSDir(hdfsPathStr: String): Unit = {
           val hdfs = FileSystem.get(new URI("hdfs://192.168.86.41:9000"), new Configuration())
           val outputDir = new Path(hdfsPathStr)
           if (hdfs.exists(outputDir))
             hdfs.delete(outputDir, true)
         }
}