package positionRulesInterface

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.io.{File, PrintWriter}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import iflytek.HomeOwnership;

/**
  * 本程序的主要目的是进行春节期间在芜湖停留天数的判断
  * 程序与judgeTravelOrLocal的程序类似  读取的数据源都是原始数据
  * 只是本程序最后保存的数据格式不是该用户出现的最早时间和最晚时间的时间差,而是保存的是在这段时间内的出现天数
  */
object PositionRules2 {



  def main(args: Array[String]) {
    //      System.setProperty("hadoop.home.dir","D:\\lib\\hadoop")
    val conf = new SparkConf().setAppName("wjchen")
      //----------------------公司-----------------------
      .setMaster("spark://192.168.86.41:7077")
      .setJars(List("/Users/chenwuji/Workspace/out/artifacts/wj/wj.jar","/Users/chenwuji/Desktop/iflytek-home-0.1.jar"))
      .set("spark.executor.memory", "10g")//Application Properties。默认1g
      .set("spark.executor.cores", "8")//Execution Behavior。默认全部
      .set("spark.cores.max","80");
    val sc = new SparkContext(conf)
    print("Begin Program");


    val allData = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/MarchTraveller.txt").map(x => x.split("\t")).map(x=>(x(0),x(1))).cache()
    val result = HomeOwnership.matchingHome(sc,allData);
    result.take(100).foreach(println);




    sc.stop()
  }


  def matchingHomeWhileTransfer(sc: SparkContext,rDD: RDD[(String,String)]):RDD[(String,(String,String),String)]={
    val result = matchingHome(sc,rDD);
    val result2 = matchingHomeWhileTransfer1(sc,result)
    result2
  }

  def matchingHome(sc: SparkContext, rDD: RDD[(String,String)]):RDD[(String,(String,String),String)]={
    //    val g4 = rDD.map(x=>x._2);
    //    val g23 = rDD.map(x=>x._2);
    val g4 = rDD.filter(x=>x._2=="4G").map(x=>x._1);
    val g23 = rDD.filter(x=>x._2=="23G").map(x=>x._1);

    /*匹配4G Emic*/
    val rules4G = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/done4GEMICResult6.txt").map(x=>x.split(",")).map(x=>(x(0),(x(1),x(2)))).cache()

    val g4_12 = g4.map(x=>(x.substring(0,12),x)).leftOuterJoin(rules4G).map(x=>{
      x._2._2 match {
        case Some((p_name, c_name)) => (x._2._1, p_name, c_name)
        case None => (x._2._1, "null", "null")
      }
    }).cache()
    val g4_success = g4_12.filter(x=>x._2!="null").map(x=>(x._1,(x._2,x._3),"4G")).cache()
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
    val g3_success = g3_11.filter(x=>x._2!="null").map(x=>(x._1,(x._2,x._3),"23G"))
    val g3_failure = g3_11.filter(x=>x._2=="null").map(x=>(x._1,x._2)).cache()
    /***结束23G匹配***/

    val result = g4_success.union(g3_success);

    result
  }


  private def deleteHDFSDir(hdfsPathStr: String): Unit = {
    val hdfs = FileSystem.get(new URI("hdfs://192.168.86.41:9000"), new Configuration())
    val outputDir = new Path(hdfsPathStr)
    if (hdfs.exists(outputDir))
      hdfs.delete(outputDir, true)
  }

  private def matchingHomeWhileTransfer1(sc: SparkContext,rDD: RDD[(String,(String,String),String)]):RDD[(String,(String,String),String)]={
      val g4 = rDD.filter(x=>x._3=="4G").cache()
      val g3ToBeTransferred = rDD.filter(x=>x._3=="23G").map(x=>(x._1,x._2)).cache()
      val g23Tog4 = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/4GIMSI_23GIMSI_MAP.txt").map(x => x.split("\t")).map(x => (x(1),x(0))).cache()
      /*尝试将23gEmic转换为4gEmic*/
      val map23gTo4g = g3ToBeTransferred.leftOuterJoin(g23Tog4).map(x=>{
        x._2._2 match{
          case Some(g4Emic) => (x._1,x._2._1,g4Emic)//前面是3gEmic  后面是转换后的4g Emic  转换得到
          case None=>(x._1,x._2._1,"23G")
        }
      }).cache()
      println("*********23G到4g:"+map23gTo4g.filter(x=>x._2!="null").count())
      map23gTo4g.map(x=>(x._1,x._2,x._3)).union(g4)

  }



}