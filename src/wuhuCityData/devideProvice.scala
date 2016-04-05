package wuhuCityData

import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计各省旅游人数的多少
  */
object devideProvice {
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

       val mappingrdd2 = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/done4GEMICResult6.txt")
         .map(x => x.split(","))
         .filter(_.length==3).map(x=>(x(0),(x(1),x(2)))).cache()
       println("4G规则映射表:"+mappingrdd2.count())

       val all123 = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/resultnew.txt")
//3g匹配数量
//       val allrdd = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/resultnew.txt").map(x => x.split("\t"))
//         .map(x => (x(1).substring(0,11),x(0))).leftOuterJoin(mappingrdd).map(x => {
//                   x._2._2 match {
//                     case Some((p_name, c_name)) => (x._2._1, p_name, c_name)
//                     case None => (x._2._1, "null", "null")
//                   }
//                   }).cache()

       val allrdd = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/resultnew.txt").map(x => x.split("\t"))
         .map(x => (x(1).substring(0,12),x(0))).leftOuterJoin(mappingrdd2).map(x => {
         x._2._2 match {
           case Some((p_name, c_name)) => (x._2._1, p_name, c_name)
           case None => (x._2._1, "null", "null")
         }
       }).cache()


       println(allrdd.count());
       print("22333322222222");
       val proviceRdd = allrdd.groupBy(_._2).map(x=>(x._1, x._2.toList.length))
       proviceRdd.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/ProviceCount1_4G")
       val anHuiRDD = allrdd.filter(_._2 == "安徽")
       val cityRdd = anHuiRDD.groupBy(_._3).map(x=>(x._1, x._2.toList.length));
       cityRdd.foreach(println);
       cityRdd.saveAsTextFile("hdfs://192.168.86.41:9000/user/wjchen/CityCount2_4G")


       println(allrdd.filter(_._2 == "null").count())
       println(allrdd.filter(_._3 == "南京").count());
       println(allrdd.filter(_._3 == "合肥").count());
       println(allrdd.filter(_._3 == "芜湖").count());
       println(allrdd.filter(_._2 == "安徽").count());



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