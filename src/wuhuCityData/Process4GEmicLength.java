package wuhuCityData;

import localJudge.Const;

import java.io.*;
import java.util.*;

/**
 * Created by chenwuji on 16/3/28.
 * 处理Emic长度问题
 */


public class Process4GEmicLength {
    public static void main(String args[])
    {
        ArrayList<Node> nodeList = loadFile("/Users/chenwuji/Documents/Rules1616.txt");
        ArrayList<Node> nodeResult =new ArrayList<>();
        for(Node i:nodeList)
        {
            if(String.valueOf(i.getEmic()).length()==8)
            {
                for(int j=0;j<10*10*10*10;j++)
                {
                    nodeResult.add(new Node(i.getEmic()*10000+j,i.getProvice(),i.getCity()));
                }
            }
            else if(String.valueOf(i.getEmic()).length()==9)
            {
                for(int j=0;j<10*10*10;j++)
                {
                    nodeResult.add(new Node(i.getEmic()*1000+j,i.getProvice(),i.getCity()));
                }
            }
            else if(String.valueOf(i.getEmic()).length()==10)
            {
                for(int j=0;j<10*10;j++)
                {
                    nodeResult.add(new Node(i.getEmic()*100+j,i.getProvice(),i.getCity()));
                }
            }
            else if(String.valueOf(i.getEmic()).length()==11)
            {
                for(int j=0;j<10;j++)
                {
                    nodeResult.add(new Node(i.getEmic()*10+j,i.getProvice(),i.getCity()));
                }
            }
            else if(String.valueOf(i.getEmic()).length()==12)
            {
                for(int j=0;j<1;j++)
                {
                    nodeResult.add(new Node(i.getEmic()+j,i.getProvice(),i.getCity()));
                }
            }
            else{
                System.out.println("奇葩");
            }
        }
        HashMap<Long,String> haha =new HashMap<>();int count=0;
        try{
            FileWriter wr = new FileWriter("/Users/chenwuji/Documents/output/done4GEMICResult6.txt",false);
            for(Node j:nodeResult)
            {
                //wr.write(j.getEmic()+","+j.getPlace()+"\n");
                haha.put(j.getEmic(),j.getProvice()+","+j.getCity());
                count++;
            }
           Iterator<Map.Entry<Long,String>> it = haha.entrySet().iterator();

            while(it.hasNext())
            {
                Map.Entry<Long,String> temp = it.next();
                wr.write(temp.getKey().toString()+","+temp.getValue().toString()+"\n");
            }
            wr.close();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        System.out.println(haha.size()+"     "+count);

    }

    private static ArrayList<Node> loadFile(String filePath) {
        ArrayList<Node> singerList = new ArrayList<Node>();
        File f = new File(filePath);
        if (f.isFile() && f.exists()) {
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
                    String lineS[] = line.split(",");
                    singerList.add(new Node(Long.valueOf(lineS[0]),lineS[1],lineS[2]));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                io.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return singerList;
    }


}

class Node{
    private long Emic;
    private String provice;
    private String city;

    public Node(long emic, String provice, String city) {
        Emic = emic;
        this.provice = provice;
        this.city = city;
    }

    public String getProvice() {
        return provice;
    }

    public void setProvice(String provice) {
        this.provice = provice;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public long getEmic() {
        return Emic;
    }

    public void setEmic(long emic) {
        Emic = emic;
    }
}
