package wuhuCityData

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将Date1中的日期范围帅选出来,找到其中在任意一天中出现次数大于两小时的数据   这部分人可以被看作是常住人口(这段日期内的)  同时统计出这段时间内的常住人口和路过人口
  * 这是第一步处理,接收的数据是原始数据  保存这段时间内的长期真正停留了芜湖的人
  * 这个程序有两个几乎一样的代码  只是为了方便并行读取操作
  */

object JudgeLocalOrTravel2 {
     def main(args: Array[String]) {
       //      System.setProperty("hadoop.home.dir","D:\\lib\\hadoop")   //windows环境下运行需要加上这部分注释
            val conf = new SparkConf().setAppName("wjchen2")
            //----------------------公司-----------------------
            .setMaster("spark://192.168.86.41:7077")
            .setJars(List("/Users/chenwuji/Workspace/out/artifacts/wj/wj.jar"))
            .set("spark.executor.memory", "10g")//Application Properties。默认1g
            .set("spark.executor.cores", "8")//Execution Behavior。默认全部
            .set("spark.cores.max","80");
            val sc = new SparkContext(conf)
            print("Begin Program");


       for(Date1 <- 20160307 to 20160320)
       {
         val idDate = sc.textFile("hdfs://192.168.86.41:9000/user/ibs/data/" + Date1 + "/*") //获得2月4号当天的所有用户信息
           .map(x => x.split(",")).map(x => (x(3), x(0).substring(6, 14).toInt)).cache()


         idDate.take(10).foreach(println)

         val grouped = idDate
           .groupByKey().cache()
           .map(t => (t._1, (t._2.max - t._2.min)))
         val stayMoreThan2Hour = grouped.filter(_._2 >= 20000)
         val stayLessThan2Hour = grouped.filter(_._2 < 20000)
         println("******************>两小时的有"+stayMoreThan2Hour.count())
         println("+******************<两小时的有"+stayLessThan2Hour.count())
         deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/March/" + Date1 + "/")
         stayMoreThan2Hour.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/March/" + Date1 + "/")
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