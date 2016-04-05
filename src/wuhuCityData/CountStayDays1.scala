package wuhuCityData

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 本程序的主要目的是进行春节期间在芜湖停留天数的判断
  * 程序与judgeTravelOrLocal的程序类似  读取的数据源都是原始数据
  * 只是本程序最后保存的数据格式不是该用户出现的最早时间和最晚时间的时间差,而是保存的是在这段时间内的出现天数
  */
object CountStayDays1 {
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


          //val Date1=20160214
       for(Date1 <- 20160204 to 20160214)
       {
         val idDate = sc.textFile("hdfs://192.168.86.41:9000/user/ibs/data/" + Date1 + "/*") //获得2月4号当天的所有用户信息
           .map(x => x.split(",")).map(x => (x(3), x(0).substring(6, 14).toInt)).cache()


         idDate.take(10).foreach(println)

         val grouped = idDate
           .groupByKey().cache()
           .map(t => (t._1, (t._2.max - t._2.min)))

         val stayMoreThan2Hour = grouped.filter(_._2 >= 20000)
         val stayLessThan2Hour = grouped.filter(_._2 < 20000)
         println("******************?>两小时的有"+stayMoreThan2Hour.count())
         println("+******************<两小时的有"+stayLessThan2Hour.count())

         val stayMoreThan2Hour1 = stayMoreThan2Hour.map(x=>(x._1,Date1));

         deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/11DAYContainsDate/" + Date1)
         stayMoreThan2Hour1.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/11DAYContainsDate/" + Date1 + "/")
         stayMoreThan2Hour.take(10).foreach(println)

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