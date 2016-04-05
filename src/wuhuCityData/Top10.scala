package wuhuCityData

import java.io.{File, PrintWriter}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *  统计省内和省外春节流入人口的数量信息,用于统计Top10流入的人   读取的数据包括23g的映射表 以及经过本地解密的手机设备信息   解密后将数据存储在HDFS上面
  */
object Top10 {
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
             println("开始1:"+mappingrdd.count())

           val mappingrdd2 = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/done4GEMICResult6.txt")
            .map(x => x.split(","))
            .filter(_.length==3).map(x=>(x(0),(x(1),x(2)))).cache()
           println("开始2:"+mappingrdd2.count())



       val allrdd = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/StayDays.txt").map(x => x.split("\t"))
         .map(x => (x(1).substring(0,11),x(2))).leftOuterJoin(mappingrdd).map(x => {
                   x._2._2 match {
                     case Some((p_name, c_name)) => (x._1,x._2._1, p_name, c_name)
                     case None => (x._2._1,"null", "null", "null")
                   }
                   }).cache()
       print("2222222222222");
       allrdd.take(100).foreach(println)
       println(allrdd.count());
       print("22333322222222");
       val shengNei = allrdd.filter(_._3 == "安徽").groupBy(_._2).map(x=>(x._1,x._2.toList.length));
       val shengWai = allrdd.filter(_._3 != "安徽").filter(_._3 != "null").groupBy(_._2).map(x=>(x._1,x._2.toList.length));
       deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/ShengNeiStayDays")
       deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/ShengWaiStayDays")
       shengNei.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/ShengNeiStayDays")
       shengWai.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/ShengWaiStayDays")



            sc.stop()
     }
  def deleteHDFSDir(hdfsPathStr: String): Unit = {
    val hdfs = FileSystem.get(new URI("hdfs://192.168.86.41:9000"), new Configuration())
    val outputDir = new Path(hdfsPathStr)
    if (hdfs.exists(outputDir))
      hdfs.delete(outputDir, true)
  }
}