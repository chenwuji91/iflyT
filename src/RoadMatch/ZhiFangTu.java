package RoadMatch;



import java.io.*;
import java.util.*;

/**
 * Created by chenwuji on 16/4/13.
 */
public class ZhiFangTu {
    private static TreeMap<Integer,Integer> map = new TreeMap<Integer,Integer>();
    public static void main(String args[]) throws IOException {


        File file = new File("/Users/chenwuji/Documents/countTill0413ZhiFangTu/");
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
        for(String i:filelist2)
        {
            System.out.println(i);
            File f = new File(rootpath+"/"+i);
            if(f.isFile()&&f.exists())
            {

                InputStreamReader io = new InputStreamReader(new FileInputStream(f),"ASCII");
                BufferedReader rd = new BufferedReader(io);
                String line = null;
                while((line=rd.readLine())!=null)
                {
                    line = line.substring(1,line.length()-1);
                    map.put(Integer.valueOf(line.split(",")[0]),Integer.valueOf(line.split(",")[1]));
                }
                io.close();

            }


        }
        System.out.println(map);
        FileWriter wr = new FileWriter("/Users/chenwuji/Documents/output/ZhiFangTu.txt",true);
        for(int k=0;k<map.size();k++)
        {
            if(map.get(k)!=null)
                 wr.write(k+","+map.get(k)+"\n");
        }
        wr.close();
    }

}
