package wuhuCityData;

import encreption.AESSecurityUtil;
import encreption.Main;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Created by chenwuji on 16/3/25.
 */
public class Count23GAnd4gUser {

    private static HashMap<String, String> g23TOg4 = new HashMap<String, String>();
    private static ArrayList<String> g23List = new ArrayList<String>();
    private static ArrayList<String> g4List = new ArrayList<String>();



    public static void main(String args[]) {
        Count23GAnd4gUser m = new Count23GAnd4gUser();
        m.getMapList();;
        System.out.println("完成映射表读取");
        m.get23g();
        System.out.println("完成23G读取");
        m.get4g();
        System.out.println("完成4G读取");
        System.out.println("初始的时刻23g用户数量是" + g23List.size());
        System.out.println("初始时刻4g用户数量是" + g4List.size());
        System.out.println("映射表的数量是" + g23TOg4.size());
        int count = 0;
        ArrayList<String> g4MappedResult = new ArrayList<>();
        Iterator<String> it = g23List.iterator();
        while (it.hasNext()) {
            String current = it.next();
            if (g23TOg4.containsKey(current)) {
                g4MappedResult.add(g23TOg4.get(current));
                count++;
            }
        }
        System.out.println("successfully mapped to 4G count" + count);
        int before = 0;int before2 =0;
        System.out.println(before = g4List.size());
        for (String i :g4MappedResult){
            if(g4List.contains(i))
            {
                before2++;
            }
        }
        System.out.println("最终的交叉用户数"+before2);

    }

    private void get4g() {

        File f = new File("/Users/chenwuji/Documents/output/4GEMIC.txt");
        if (f.isFile() && f.exists()) {

            InputStreamReader io = null;
            try {
                io = new InputStreamReader(new FileInputStream(f));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            BufferedReader rd = new BufferedReader(io);
            String line = null;
            try {
                while ((line = rd.readLine()) != null) {
                    g4List.add(line);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                io.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    private void get23g() {

            File f = new File("/Users/chenwuji/Documents/output/23GEMIC.txt");
            if (true) {
                //System.out.println("begin");
                InputStreamReader io = null;
                try {
                    io = new InputStreamReader(new FileInputStream(f));
                }  catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
                BufferedReader rd = new BufferedReader(io);
                String line = null;
                try {
                    while ((line = rd.readLine()) != null) {
                       // System.out.println(line);
                        try{
                        g23List.add(line);
                        }
                        catch(ArrayIndexOutOfBoundsException e)
                        {
                            e.printStackTrace();
                            System.out.println(line);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    io.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
    }
///Users/chenwuji/Documents/output/23GEMIC.txt


    private void getMapList() {

            List<String> result = new ArrayList<String>();
            File f = new File("/Users/chenwuji/Documents/234g/4GIMSI_23GIMSI_MAP.txt");
            int countMap =0;
            if (f.isFile() && f.exists()) {

                InputStreamReader io = null;
                try {
                    io = new InputStreamReader(new FileInputStream(f), "ASCII");
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
                BufferedReader rd = new BufferedReader(io);
                String line = null;
                try {
                    while ((line = rd.readLine()) != null) {
                        countMap++;
                        String g4Emic = line.split("\t")[0];
                        String g3Emic = line.split("\t")[1];
                        g23TOg4.put(g3Emic, g4Emic);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    io.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

//            }
                System.out.println("映射数量"+ countMap);

        }
    }
}
