package localJudge;

import encreption.AESSecurityUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by chenwuji on 16/3/29.
 * 本程序主要是将Spark集群生成的文件 生成测试文件的标准格式
 * 集群生成的文件为每个日期一个文件夹,在testResult目录下面
 * 每个日期目录下包含有所有歌手当天的收听次数
 * 该程序独立于该包下的其他类,独立运行
 */
public class Gather {
    public static void main(String args[]) throws Exception {
//        File file = new File("/Users/chenwuji/Documents/skypool/allSingerData/skypoolAllSinger");
        File file = new File("/Users/chenwuji/Documents/skypool/dayBydayFinal/AllResult/");
//        /Users/chenwuji/Documents/skypool/SongTestSet
        String rootpath = file.getAbsolutePath();
        String fileList[] = file.list();
        for (String i : fileList) {
            File f = new File(rootpath + File.separator + i);
            if (f.isDirectory()) {
                writeOneFile(rootpath + File.separator + i,i);
             //   System.out.println(i);
            }
        }


    }

    private static  void writeOneFile(String path, String date) throws Exception{
        int count = 0;
        File file = new File(path);
        String rootpath = file.getAbsolutePath();
        String fileList[] = file.list();
        ArrayList<String> filelist2 = new ArrayList<String>();
        for (String ii : fileList) {
            if (ii.contains("part")) {
                filelist2.add(ii);
            }
        }
        for (String i : filelist2) {
           // System.out.println(i);
            List<String> result = new ArrayList<String>();
            File f = new File(rootpath + "/" + i);
            if (f.isFile() && f.exists()) {

                InputStreamReader io = new InputStreamReader(new FileInputStream(f), "ASCII");
                BufferedReader rd = new BufferedReader(io);
                String line = null;
                while ((line = rd.readLine()) != null) {

                    if(line.length()<2)
                        break;
                    count++;
                    line = line.substring(1, line.length() - 1);
                    result.add(line.split(",")[0] + "," + line.split(",")[1] + "," + date);
                }
                io.close();

            }
            FileWriter wr = new FileWriter("/Users/chenwuji/Documents/skypool/SongTestSet2.txt", true);

            for (String j : result) {
                wr.write(j + "\n");
            }
            wr.close();

        }
        if(count!=50)
            System.out.println("warning!"+date+" "+count);
    }
}