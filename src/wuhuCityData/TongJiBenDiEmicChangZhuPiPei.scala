package wuhuCityData

import java.io.{File, PrintWriter}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计本地Emic的匹配率
  */
object TongJiBenDiEmicChangZhuPiPei {
     def main(args: Array[String]) {
            val conf = new SparkConf().setAppName("test456")
            //----------------------公司-----------------------
            .setMaster("spark://192.168.86.41:7077")
            .setJars(List("/Users/chenwuji/Workspace/out/artifacts/wj/wj.jar"))
            .set("spark.executor.memory", "10g")//Application Properties。默认1g
            .set("spark.executor.cores", "8")//Execution Behavior。默认全部
            .set("spark.cores.max","80");
            val sc = new SparkContext(conf)
            print("Begin Program");
      // /user/wjchen/4GIMSI_23GIMSI_MAP.txt
      ///user/wjchen/LocalEmic0414.txt
       val mappingTable = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/4GIMSI_23GIMSI_MAP.txt").map(x=>x.split("\t")).map(x=>x(0)).cache();
       //mappingTable.take(100).foreach(println)
       println("4G映射的总数是:"+mappingTable.count())
       val localEmic = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/LocalEmic0414.txt").cache();
       println("本地4g EMIC 的总数是:"+localEmic.count())
       val NotMathched = tongjiIntersection(sc,mappingTable,localEmic)
       println("没有匹配上的的总数是:"+NotMathched.count())
       deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/NotMatchedEmic0414")

       NotMathched.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/NotMatchedEmic0414")
       sc.stop()
     }

      def tongjiIntersection(sparkContext: SparkContext,mapping: RDD[String],localEmic: RDD[String]):RDD[String] = {
//        val notmatched = localEmic.subtract(mapping);
//        notmatched
        val matched = localEmic.intersection(mapping);
        val notM = localEmic.subtract(matched)
        notM
      }

  def deleteHDFSDir(hdfsPathStr: String): Unit = {
    val hdfs = FileSystem.get(new URI("hdfs://192.168.86.41:9000"), new Configuration())
    val outputDir = new Path(hdfsPathStr)
    if (hdfs.exists(outputDir))
      hdfs.delete(outputDir, true)
  }
}