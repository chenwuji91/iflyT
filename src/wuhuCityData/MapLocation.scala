//package distribution
//import org.apache.spark.{ SparkConf, SparkContext }
//import org.apache.spark.SparkContext._
//object Combines {
//     def main(args: Array[String]) {
//             System.setProperty("hadoop.home.dir","D:\\lib\\hadoop")
//            val conf = new SparkConf().setAppName("wjchen")
//            //----------------------公司-----------------------
//            .setMaster("spark://192.168.86.41:7077")
//            .setJars(List("file:///D:/lib/Wjchen.jar"))
//            .set("spark.executor.memory", "10g")//Application Properties。默认1g
//            .set("spark.executor.cores", "8")//Execution Behavior。默认全部
//            .set("spark.cores.max","80");
//            val sc = new SparkContext(conf)
//            print("Begin");
//
//     val allrdd = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/result.txt").map(x => x.split("  ")).map(x => (x(1).substring(0,11),x(0))).leftOuterJoin(mappingrdd).map(x => {
//         x._2._2 match {
//           case Some((p_name, c_name)) => (x._2._1, p_name, c_name)
//           case None => (x._2._1, "null", "null")
//         }
//         }).cache()
//    val writer = new PrintWriter(new File("D://归属地统计.txt"))
//    writer.println(allrdd.count())
//    writer.println(allrdd.filter(_._3 == "南京").count())
//    writer.println(allrdd.filter(_._3 == "合肥").count())
//    writer.println(allrdd.filter(_._3 == "芜湖").count())
//    writer.close()
//
//
//
//            sc.stop()
//     }
//}