package wuhuCityData
import java.io.{PrintWriter, PrintStream}
import java.net.URI
import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * 读取23g映射表以及解密后的数据  统计部分省市的人口流入的情况,输出相关归属地的人数信息  保存相关信息到本地
  * 本程序中间部分数据集需要本地解密后在重新上传到hdfs上进行操作
  */
object CityAndProviceTravelPeopleCount {
     def main(args: Array[String]) {
       //      System.setProperty("hadoop.home.dir","D:\\lib\\hadoop")
            val conf = new SparkConf().setAppName("test456")
            //----------------------公司-----------------------
            .setMaster("spark://192.168.86.41:7077")
            .setJars(List("/Users/chenwuji/Workspace/out/artifacts/wj/wj.jar"))
            .set("spark.executor.memory", "10g")//Application Properties。默认1g
            .set("spark.executor.cores", "8")//Execution Behavior。默认全部
            .set("spark.cores.max","80");
            val sc = new SparkContext(conf)
            print("Begin Program");


          val mappingrdd = sc.textFile("hdfs://192.168.86.41:9000/user/ekyang/Mapping/23gMap.txt")
            .map(x => x.split("\t"))
            .filter(_.length==3).map(x=>(x(0),(x(1),x(2)))).cache()

          val all123 = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/MarchTraveller.txt")
          println("ALL********"+all123.count());
          val allrdd = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/MarchTraveller.txt").map(x => x.split("\t"))
          .map(x => (x(1).substring(0,11),x(0))).leftOuterJoin(mappingrdd).map(x => {
                   x._2._2 match {
                     case Some((p_name, c_name)) => (x._2._1, p_name, c_name)
                     case None => (x._2._1, "null", "null")
                   }
                   }).cache()

           println(" "+allrdd.filter(_._2 == "安徽").count());
           println(" "+allrdd.filter(_._2 != "安徽").filter(_._2 != "null").count());



              val writer = new PrintWriter(new File("/Users/chenwuji/Documents/output/statistic.txt"))
              writer.println("全部:"+allrdd.count())
              writer.println("南京:"+allrdd.filter(_._3 == "南京").count())
              writer.println("合肥:"+allrdd.filter(_._3 == "合肥").count())
              writer.println("芜湖:"+allrdd.filter(_._3 == "芜湖").count())
              writer.println("江苏:"+allrdd.filter(_._2 == "江苏").count())
              writer.println("安徽:"+allrdd.filter(_._2 == "安徽").count())
              writer.close()




            sc.stop()
     }
}