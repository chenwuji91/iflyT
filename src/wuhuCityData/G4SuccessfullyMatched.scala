package wuhuCityData

import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import java.io.{PrintWriter, PrintStream}
import java.net.URI
import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * 观察能够成功匹配到的用户数量
  */
object G4SuccessfullyMatched {
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

    val g4 = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/g4EMIC2").map(x=>x.toString).filter(x=>x.length>12)
    val rules = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/done4GEMICResult5.txt").map(x=>x.split(",")).map(x=>(x(0),x(1)))
    rules.take(20).foreach(println)
    println("*****************")
    g4.take(100).foreach(println)

    val g4_7 = g4.map(x=>(x.substring(0,7),"null")).leftOuterJoin(rules).map(x=>{
      x._2._2 match {
        case Some(name) => (x._1, name)
        case None => (x._1,"null")
      }
    }).cache()
    val g4_8 = g4.map(x=>(x.substring(0,8),"null")).leftOuterJoin(rules).map(x=>{
      x._2._2 match {
        case Some(name) => (x._1, name)
        case None => (x._1,"null")
      }
    }).cache()
    val g4_9 = g4.map(x=>(x.substring(0,9),"null")).leftOuterJoin(rules).map(x=>{
      x._2._2 match {
        case Some(name) => (x._1, name)
       case None => (x._1,"null")
      }
    }).cache()
    val g4_10 = g4.map(x=>(x.substring(0,10),"null")).leftOuterJoin(rules).map(x=>{
      x._2._2 match {
        case Some(name) => (x._1, name)
        case None => (x._1,"null")
      }
    }).cache()
    val g4_11 = g4.map(x=>(x.substring(0,11),"null")).leftOuterJoin(rules).map(x=>{
      x._2._2 match {
        case Some(name) => (x._1, name)
        case None => (x._1,"null")
      }
    }).cache()
    val g4_12 = g4.map(x=>(x.substring(0,12),"null")).leftOuterJoin(rules).map(x=>{
      x._2._2 match {
        case Some(name) => (x._1, name)
        case None => (x._1,"null")
      }
    }).cache()
    g4_12.take(1000).foreach(println)


    //      .distinct().intersection(rules).count();
//    val g4_8 = g4.map(x=>x.substring(0,8)).distinct().intersection(rules).count();
//    val g4_9 = g4.map(x=>x.substring(0,9)).distinct().intersection(rules).count();
//    val g4_10 = g4.map(x=>x.substring(0,10)).distinct().intersection(rules).count();
//    val g4_11 = g4.map(x=>x.substring(0,11)).distinct().intersection(rules).count();
//    val g4_12 = g4.map(x=>x.substring(0,12)).distinct().intersection(rules).count();


    val writer = new PrintWriter(new File("/Users/chenwuji/Documents/output/成功匹配到的4g用户数"))
    writer.println("全部4g用户数:"+g4.count())
    writer.println("七位4g用户数:"+g4_7.filter(x=>x._2!="null").count())
    writer.println("八位4g用户数:"+g4_8.filter(x=>x._2!="null").count())
    writer.println("九位4g用户数:"+g4_9.filter(x=>x._2!="null").count())
    writer.println("十位4g用户数:"+g4_10.filter(x=>x._2!="null").count())
    writer.println("十一位4g用户数:"+g4_11.filter(x=>x._2!="null").count())
    writer.println("十二位4g用户数:"+g4_12.filter(x=>x._2!="null").count())
    writer.println("全部匹配上4g用户数:"+(g4_7.filter(x=>x._2!="null").count()+g4_8.filter(x=>x._2!="null").count()+g4_9.filter(x=>x._2!="null").count()
      +g4_10.filter(x=>x._2!="null").count()+g4_11.filter(x=>x._2!="null").count()+g4_12.filter(x=>x._2!="null").count()))
    writer.close()
    println(g4.count())
    deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/20150310MappedToPlaces/")
    deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/20150310NotMatchedMappedToPlaces/")
    g4_12.filter(x=>x._2!="null").saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/20150310MappedToPlaces/")
    g4_12.filter(x=>x._2=="null").saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/20150310NotMatchedMappedToPlaces/")



    sc.stop()
  }
  def deleteHDFSDir(hdfsPathStr: String): Unit = {
    val hdfs = FileSystem.get(new URI("hdfs://192.168.86.41:9000"), new Configuration())
    val outputDir = new Path(hdfsPathStr)
    if (hdfs.exists(outputDir))
      hdfs.delete(outputDir, true)
  }
}