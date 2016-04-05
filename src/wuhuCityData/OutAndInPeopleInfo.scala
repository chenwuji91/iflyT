package wuhuCityData

import java.io.{PrintWriter, PrintStream}
import java.net.URI
import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkContext, SparkConf}

/**
  *本程序的功能主要是实现了读取多个数据集,即多个在一段时间内长期停留在这里的人口， 然后通过取差集\交集  获得春节在这里的本地人   春节流出的本地人等信息
  */
object OutAndInPeopleInfo {
   def main(args: Array[String]) {
 //           System.setProperty("hadoop.home.dir","/Users/chenwuji/Documents/lib/hadoop/")
            val conf = new SparkConf().setAppName("test456")
            //----------------------公司-----------------------
            .setMaster("spark://192.168.86.41:7077")
            .setJars(List("/Users/chenwuji/Workspace/out/artifacts/wj/wj.jar"))
            .set("spark.executor.memory", "10g")//Application Properties。默认1g
            .set("spark.executor.cores", "8")//Execution Behavior。默认全部
            .set("spark.cores.max","80")
            val sc = new SparkContext(conf)
            print("Begin")


        val rdd = sc.textFile("hdfs://192.168.86.41:9000//user/wjchen/14DAY/*").map(x => x.split(",|\\)|\\(")).map(x => (x(1),x(2).toInt)).map(x => x._1).distinct().cache()//本地人rdd

        val RefToFTGetTravellerRdd = sc.textFile("hdfs://192.168.86.41:9000//user/wjchen/March/*")
               .map(x => x.split(",|\\)|\\(")).map(x => (x(1),x(2).toInt)).map(x => x._1).distinct().cache()//三月旅游RDD

        val writer = new PrintWriter(new File("/Users/chenwuji/Documents/output/11DAY.txt"))//春节旅游RDD

         writer.println("+******************总共数据"+RefToFTGetTravellerRdd.count())
         println("+******************总共数据"+RefToFTGetTravellerRdd.count())
         writer.close();

 //    val foreignTraveller = RefToFTGetTravellerRdd.intersection(rdd)
        val rdd1 = RefToFTGetTravellerRdd.subtract(rdd)    //春节在芜湖的减去本地人,就是外地旅游的人,流入人口
        println("旅游的人     "+rdd1.count())


//     val rdd2 = rdd.subtract(RefToFTGetTravellerRdd)    //本地人减去春节在芜湖的人,是流出人口
//     println("流出的人     "+rdd2.count())

       deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/MarchforeignTraveller")
       rdd1.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/MarchforeignTraveller")

//     deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/Liuchu")
//     rdd2.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/Liuchu")

            sc.stop()
            }


  def deleteHDFSDir(hdfsPathStr: String): Unit = {
    val hdfs = FileSystem.get(new URI("hdfs://192.168.86.41:9000"), new Configuration())
    val outputDir = new Path(hdfsPathStr)
    if (hdfs.exists(outputDir))
      hdfs.delete(outputDir, true)
  }
}