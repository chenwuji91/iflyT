package skypool

import java.net.URI

import java.io.{PrintWriter, PrintStream}
import java.net.URI
import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkContext, SparkConf}
object CountUser {
     def main(args: Array[String]) {
       //      System.setProperty("hadoop.home.dir","D:\\lib\\hadoop")
            val conf = new SparkConf().setAppName("test3")
            //----------------------公司-----------------------
            .setMaster("spark://192.168.86.41:7077")
            .setJars(List("/Users/chenwuji/Workspace/out/artifacts/wj/wj.jar"))
            .set("spark.executor.memory", "10g")//Application Properties。默认1g
            .set("spark.executor.cores", "8")//Execution Behavior。默认全部
            .set("spark.cores.max","80");
            val sc = new SparkContext(conf)
            print("Begin Program");



       val userAction = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/skyPoolUserAction")
           .map(x => x.split(",")).map(x=>(x(0),x(1),TransUnixToDateTime.translate(x(2).toInt).toInt,x(3).toInt,x(4).toInt)).cache()

       val userAction3 = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/skyPoolUserAction")
         .map(x => x.split(",")).map(x=>(x(0),x(1),TransUnixToDateTime.translate(x(2).toInt).toInt,x(3).toInt,x(4).toInt)).filter(x=>x._3>2015030000).filter(x=>x._3<2015033200).cache()
       val userAction4 = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/skyPoolUserAction")
         .map(x => x.split(",")).map(x=>(x(0),x(1),TransUnixToDateTime.translate(x(2).toInt).toInt,x(3).toInt,x(4).toInt)).filter(x=>x._3>2015040000).filter(x=>x._3<2015043200).cache()
       val userAction5 = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/skyPoolUserAction")
         .map(x => x.split(",")).map(x=>(x(0),x(1),TransUnixToDateTime.translate(x(2).toInt).toInt,x(3).toInt,x(4).toInt)).filter(x=>x._3>2015050000).filter(x=>x._3<2015053200).cache()
       val userAction6 = sc.textFile("hdfs://192.168.86.41:9000/user/wjchen/skyPoolUserAction")
         .map(x => x.split(",")).map(x=>(x(0),x(1),TransUnixToDateTime.translate(x(2).toInt).toInt,x(3).toInt,x(4).toInt)).filter(x=>x._3>2015060000).filter(x=>x._3<2015063200).cache()




       val userCount = userAction.map(x=>x._1).distinct().count();
       val writer = new PrintWriter(new File("/Users/chenwuji/Documents/output/TianChiUserCal3.txt"))
       writer.println("User Count: "+userCount)
       writer.println("User Count 3: "+userAction3.map(x=>x._1).distinct().count());
       writer.println("User Count 4: "+userAction4.map(x=>x._1).distinct().count());
       writer.println("User Count 5: "+userAction5.map(x=>x._1).distinct().count());
       writer.println("User Count 6: "+userAction6.map(x=>x._1).distinct().count());

       writer.println("3 intersection 4: "+userAction3.map(x=>x._1).distinct().intersection(userAction4.map(x=>x._1).distinct()).count());
       writer.println("5 intersection 6: "+userAction5.map(x=>x._1).distinct().intersection(userAction6.map(x=>x._1).distinct()).count());
       writer.println("4 intersection 6: "+userAction4.map(x=>x._1).distinct().intersection(userAction6.map(x=>x._1).distinct()).count());
       writer.println("3 intersection 6: "+userAction3.map(x=>x._1).distinct().intersection(userAction6.map(x=>x._1).distinct()).count());
       writer.println("3 intersection 4,5,6: "+userAction3.map(x=>x._1).distinct()
         .intersection(userAction6.map(x=>x._1).distinct())
         .intersection(userAction4.map(x=>x._1).distinct())
         .intersection(userAction5.map(x=>x._1).distinct()).count());


         writer.close()

         sc.stop()
     }
  def deleteHDFSDir(hdfsPathStr: String): Unit = {
           val hdfs = FileSystem.get(new URI("hdfs://192.168.86.41:9000"), new Configuration())
           val outputDir = new Path(hdfsPathStr)
           if (hdfs.exists(outputDir))
             hdfs.delete(outputDir, true)
         }
}