//package distribution
//
//import java.io.{PrintWriter, PrintStream}
//import java.net.URI
//import java.io.File
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.{Path, FileSystem}
//import org.apache.spark.{SparkContext, SparkConf}
//
//object RemoveForeign {
//     def main(args: Array[String]) {
//       //      System.setProperty("hadoop.home.dir","D:\\lib\\hadoop")
//            val conf = new SparkConf().setAppName("wjchen")
//            //----------------------公司-----------------------
//            .setMaster("spark://192.168.86.41:7077")
//            .setJars(List("/Users/chenwuji/Workspace/out/artifacts/wj/wj.jar"))
//            .set("spark.executor.memory", "10g")//Application Properties。默认1g
//            .set("spark.executor.cores", "8")//Execution Behavior。默认全部
//            .set("spark.cores.max","80");
//            val sc = new SparkContext(conf)
//            print("Begin Program");
//
//
//          //val Date1=20160214
//       for(Date1 <- 20160204 to 20160214)
//       {
//         val idDate = sc.textFile("hdfs://192.168.86.41:9000/user/ibs/data/" + Date1 + "/*") //获得2月4号当天的所有用户信息
//           .map(x => x.split(",")).map(x => (x(3), x(0).substring(6, 14).toInt)).cache()
//
//
//         idDate.take(10).foreach(println)
//
//         val grouped = idDate
//           .groupByKey().cache()
//           .map(t => (t._1, (t._2.max - t._2.min)))
//
//         val stayMoreThan2Hour = grouped.filter(_._2 >= 20000)
//         val stayLessThan2Hour = grouped.filter(_._2 < 20000)
//         println("******************?>两小时的有"+stayMoreThan2Hour.count())
//         println("+******************<两小时的有"+stayLessThan2Hour.count())
//
//         deleteHDFSDir("hdfs://192.168.86.41:9000/user/wjchen/11DAY/" + Date1)
//         stayMoreThan2Hour.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/11DAY/" + Date1 + "/")
//         stayMoreThan2Hour.take(10).foreach(println)
////        val writer = new PrintWriter(new File("/Users/chenwuji/Documents/output/deleteRatio"+Date1+".txt"))
////         writer.println("+******************>两小时的有"+stayMoreThan2Hour.count())
////         writer.println("+******************<两小时的有"+stayLessThan2Hour.count())
////
////         writer.close()
//       }
//         sc.stop()
//     }
//  def deleteHDFSDir(hdfsPathStr: String): Unit = {
//           val hdfs = FileSystem.get(new URI("hdfs://192.168.86.41:9000"), new Configuration())
//           val outputDir = new Path(hdfsPathStr)
//           if (hdfs.exists(outputDir))
//             hdfs.delete(outputDir, true)
//         }
//}