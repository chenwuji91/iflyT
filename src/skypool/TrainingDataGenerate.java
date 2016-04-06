package skypool;

import encreption.AESSecurityUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;

/**
 * Created by chenwuji on 16/3/30.
 * 训练数据集的规范化生成
 */
public class TrainingDataGenerate {
    public static HashMap<String,String> map = new HashMap();
    public static void main(String args[]) throws Exception {
        generateBySong();
    }
    private static HashMap<String,String> singersong() throws Exception {
      //  /Users/chenwuji/Documents/skypool/mars_tianchi_songs.csv
        File f = new File("/Users/chenwuji/Documents/skypool/mars_tianchi_songs.csv");
        InputStreamReader io =new InputStreamReader(new FileInputStream(f));
        BufferedReader rd = new BufferedReader(io);
        String line = null;
        while((line=rd.readLine())!=null){
            String[] l = line.split(",");
            if(l.length>2)
            {
                map.put(l[0],l[1]);
            }
        }
        return map;
    }
    private static void generateByUser() throws  Exception
    {
        File file = new File("/Users/chenwuji/Documents/skypool/training/");
        String rootpath = file.getAbsolutePath();
        String fileList[] = file.list();
        ArrayList<String> filelist2 = new ArrayList<String>();
        for(String ii:fileList)
        {
            if(ii.contains("part"))
            {
                filelist2.add(ii);
            }
        }
        for(String i:filelist2) {
            System.out.println(i);
            List<String> result = new ArrayList<String>();
            File f = new File(rootpath + "/" + i);
            if (f.isFile() && f.exists()) {

                InputStreamReader io = new InputStreamReader(new FileInputStream(f), "ASCII");
                BufferedReader rd = new BufferedReader(io);
                String line = null;
                while ((line = rd.readLine()) != null) {

                   // System.out.println(line);
                    line = line.substring(1, line.length() - 1);
                    String date = line.split(",")[0];
                    writeToFile("/Users/chenwuji/Documents/skypool/TrainingByUser/", line, date);
                }
                io.close();
            }
        }
    }

    private static void generateBySinger() throws  Exception
    {
        singersong();
        File file = new File("/Users/chenwuji/Documents/skypool/training/");
        String rootpath = file.getAbsolutePath();
        String fileList[] = file.list();
        ArrayList<String> filelist2 = new ArrayList<String>();
        for(String ii:fileList)
        {
            if(ii.contains("part"))
            {
                filelist2.add(ii);
            }
        }
        for(String i:filelist2) {
            System.out.println(i);
            List<String> result = new ArrayList<String>();
            File f = new File(rootpath + "/" + i);
            if (f.isFile() && f.exists()) {

                InputStreamReader io = new InputStreamReader(new FileInputStream(f), "ASCII");
                BufferedReader rd = new BufferedReader(io);
                String line = null;
                while ((line = rd.readLine()) != null) {

                    //System.out.println(line);
                    line = line.substring(1, line.length() - 1);
                    String singer = map.get(line.split(",")[1]);
                    writeToFile("/Users/chenwuji/Documents/skypool/TrainingBySinger/", line, singer);
                }
                io.close();
            }
        }
    }

    private static void generateBySong() throws  Exception
    {
        File file = new File("/Users/chenwuji/Documents/skypool/skypool_testing_data/");
        String rootpath = file.getAbsolutePath();
        String fileList[] = file.list();
        ArrayList<String> filelist2 = new ArrayList<String>();
        for(String ii:fileList)
        {
            if(ii.contains("part"))
            {
                filelist2.add(ii);
            }
        }
        for(String i:filelist2) {
            System.out.println(i);
            List<String> result = new ArrayList<String>();
            File f = new File(rootpath + "/" + i);
            if (f.isFile() && f.exists()) {

                InputStreamReader io = new InputStreamReader(new FileInputStream(f), "ASCII");
                BufferedReader rd = new BufferedReader(io);
                String line = null;
                while ((line = rd.readLine()) != null) {

                    System.out.println(line);
                    line = line.substring(1, line.length() - 1);
                    String date = line.split(",")[1];
                    writeToFile("/Users/chenwuji/Documents/skypool/TrainingBysongTesting/", line, date);
                }
                io.close();
            }
        }
    }
    private static void writeToFile(String path,String data,String date) throws Exception
    {
        FileWriter wr = new FileWriter(path+date+".csv",true);
        wr.write(data+"\n");
        wr.close();

    }
}
