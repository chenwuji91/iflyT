package positionRulesInterface

import java.io.{File, PrintWriter}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 本程序的主要目的是进行春节期间在芜湖停留天数的判断
  * 程序与judgeTravelOrLocal的程序类似  读取的数据源都是原始数据
  * 只是本程序最后保存的数据格式不是该用户出现的最早时间和最晚时间的时间差,而是保存的是在这段时间内的出现天数
  */
object PositionRules {
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

//    /*文件读取及预处理*/
//    val allData = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/MarchTraveller.txt").map(x => x.split(",")).map(x=>(x(0),x(1))).cache()
//    val g4 = allData.filter(x=>x._2=="4G").map(x=>x._1).filter(x=>x.length>12).cache();
//    val g23 = allData.filter(x=>x._2=="23G").map(x=>x._1).filter(x=>x.length>12).cache();
//    /*结束文件读取及预处理*/
    val allData = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/MarchTraveller.txt").map(x => x.split("\t")).map(x=>(x(0),x(1))).cache()
    val g4 = allData.map(x=>x._2);
    val g23 =allData.map(x=>x._2);

    /*匹配4G Emic*/
    val rules4G = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/done4GEMICResult6.txt").map(x=>x.split(",")).map(x=>(x(0),(x(1),x(2)))).cache()

    val g4_12 = g4.map(x=>(x.substring(0,12),x)).leftOuterJoin(rules4G).map(x=>{
      x._2._2 match {
        case Some((p_name, c_name)) => (x._2._1, p_name, c_name)
        case None => (x._2._1, "null", "null")
      }
    }).cache()
    val g4_success = g4_12.filter(x=>x._2!="null").map(x=>(x._1,(x._2,x._3),"1")).cache()
    val g4_failure = g4_12.filter(x=>x._2=="null").map(x=>(x._1,x._2)).cache()
    /***结束4G匹配***/


    /*匹配23G Emic*/
    val rules23G = sc.textFile("hdfs://192.168.86.41:9000/user/ekyang/Mapping/23gMap.txt")
      .map(x => x.split("\t"))
      .filter(_.length==3).map(x=>(x(0),(x(1),x(2)))).cache()
    val g3_11 = g23.map(x=>(x.substring(0,11),x)).leftOuterJoin(rules23G).map(x=>{
      x._2._2 match {
        case Some((p_name, c_name)) => (x._2._1, p_name, c_name)
        case None => (x._2._1, "null", "null")
      }
    }).cache()
    val g3_success = g3_11.filter(x=>x._2!="null").map(x=>(x._1,(x._2,x._3),"2"))
    val g3_failure = g3_11.filter(x=>x._2=="null").map(x=>(x._1,x._2)).cache()
    /***结束23G匹配***/


//    g3_failure.take(1000).foreach(println);

    /***未匹配项转换后匹配***/

    val g23Tog4 = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/4GIMSI_23GIMSI_MAP.txt")
      .map(x => x.split("\t")).map(x => (x(1),x(0))).cache()
    val g4Tog23 = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/4GIMSI_23GIMSI_MAP.txt")
      .map(x => x.split("\t")).map(x => (x(0),x(1))).cache()

    val ggg = g3_failure.map(x=>x._1).intersection(g23Tog4.map(x=>x._1)).count();
    println(ggg)
    val ggg2 = g3_success.map(x=>x._1).intersection(g23Tog4.map(x=>x._1)).count();
    println(ggg2)
//
    /*尝试将23gEmic转换为4gEmic*/
    val map23gTo4g = g3_failure.leftOuterJoin(g23Tog4).map(x=>{
        x._2._2 match{
          case Some(g4Emic) => (x._1,g4Emic)//前面是3gEmic  后面是转换后的4g Emic  转换得到
          case None=>(x._1,"null")
        }
    }).cache()
    println("*********23G到4g:"+map23gTo4g.filter(x=>x._2!="null").count())
//    map23gTo4g.take(100).foreach(println);
//
    /*尝试将4gEmic转换为23gEmic*/
    val map4gTo23g = g4_failure.leftOuterJoin(g4Tog23).map(x=>{
      x._2._2 match{
        case Some(g3Emic) => (x._1,g3Emic)//前面4g  后面23g转换得到
        case None => (x._1,"null")
      }
    }
    ).cache()
    println("*********4g到23g:"+map4gTo23g.filter(x=>x._2!="null").count())
//    map4gTo23g.take(100).foreach(println);
////
////map里面的参数  按照顺序   前面是原始的23GEmic  前面一个是转换得到  后面是4G的Emic 为原始   使用前面的23G的Emic作为匹配的主要规则
////    val g3_2pipei = map4gTo23g.filter(x=>x._2!="null").map(x=>(x._2.substring(0,11),x._1)).leftOuterJoin(rules23G).map(x=>{
////      x._2._2 match {
////        case Some((p_name, c_name)) => (x._2._1, p_name, c_name)
////        case None => (x._2._1, "null", "null")
////      }
////    }).cache()
////    val g3_2_success = g3_2pipei.filter(x=>x._2!="null").map(x=> (x._1,(x._2,x._3),"3"))
////    g3_2_success.take(1000).foreach(println)
////    println("***********2pipei"+g3_2_success.count())
////    val g3_2_failure = g3_2pipei.filter(x=>x._2=="null").map(x=>(x._1,x._2)).cache()
//
////前面一个参数是4g Emic  转换得到的  后面的是原始的23g的Emic  需要在这里输出转换后的Emic    23g原始 4g
////    val g4_2pipei = map23gTo4g.filter(x=>x._2!="null").map(x=>(x._2.substring(0,12),x._1)).leftOuterJoin(rules4G)
////  .map(x=>{
////      x._2._2 match {
////        case Some((p_name, c_name)) => (x._2._1, p_name, c_name )
////        case None => (x._2._1, "null", "null")
////      }
////    }).cache()
////   g4_2pipei.take(1000).foreach(println)
//
////    val g4_2_success = g4_2pipei.filter(x=>x._2!="null").map(x=> (x._1._1,(x._2,x._3),x._1._2))
////    val g4_2_failure = g4_2pipei.filter(x=>x._2=="null").map(x=>(x._1,x._2)).cache()
////    println("*****g4_success"+g4_2_success.count())
////    println("*****g4_failure"+g4_2_failure.count())
////
//    val g4321 = map23gTo4g.filter(x=>x._2!="null").map(x=>x._2);
//    val pipie2 = g4321.map(x=>(x.substring(0,12),x)).leftOuterJoin(rules4G).map(x=>{
//      x._2._2 match {
//        case Some((p_name, c_name)) => (x._2._1, p_name, c_name)
//        case None => (x._2._1, "null", "null")
//      }
//    }).cache()
//    pipie2.take(200).foreach(println)
//    println("succ***"+pipie2.filter(x=>x._2!="null").count());
//    println("failure***"+pipie2.filter(x=>x._2=="null").count());


//    val result = g4_success.union(g4_2_success).union(g3_2_success).union(g3_success);
//    val writer = new PrintWriter(new File("/Users/chenwuji/Documents/output/匹配报告.txt"))
//    writer.println("最终成功匹配:"+result.count())
//    writer.println("第一次4g成功匹配:"+g4_success.count())
//    writer.println("第二次4g成功匹配:"+g4_2_success.count())
//    writer.println("第一次3g成功匹配:"+g3_success.count())
//    writer.println("第二次3g成功匹配:"+g3_2_success.count())
//    writer.println("第一次4g失败匹配:"+g4_failure.count())
//    writer.println("第一次3g失败匹配:"+g3_failure.count())
//
//    writer.close()
//    deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/TestInterface1")
//    result.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/TestInterface1")

    sc.stop()
  }

  def deleteHDFSDir(hdfsPathStr: String): Unit = {
    val hdfs = FileSystem.get(new URI("hdfs://192.168.86.41:9000"), new Configuration())
    val outputDir = new Path(hdfsPathStr)
    if (hdfs.exists(outputDir))
      hdfs.delete(outputDir, true)
  }
}