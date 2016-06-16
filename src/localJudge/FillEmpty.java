package localJudge;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by chenwuji on 16/4/16.
 */


public class FillEmpty {
    private ArrayList<Singer> predict;
    public static final String pathToBeFilled = "/Users/chenwuji/Documents/skypool/HotSongAndColdSong/song/";
    private ArrayList<Singer> songList;

    public static void main(String args[]) throws IOException {
        FillEmpty m =new FillEmpty();
        m.init();
        m.fillData();
        m.output();
    }

    public void output() throws IOException {
        for(Singer i: predict)
        {
            String songName = i.getSingerId();
            int listenCount = i.getListenCount();
            int time = i.getDatetime();
            FileWriter wr = new FileWriter("/Users/chenwuji/Documents/skypool/HotSongAndColdSong/songFilled/"+songName+".csv",true);
            wr.write(listenCount+","+time+"\n");
            wr.close();
        }
    }

    private void init()
    {
        predict = FileRead.loadSongPeople();//加载训练数据集
        songList = FileRead.loadSongList();
        System.out.println("完成数据加载");
    }
    private void fillData()
    {

        HashMap<String,HashMap<Integer,Integer>> predictingByDate = new HashMap<>();
        HashSet<String> song = new HashSet<String>();
        for(Singer s:songList)
        {
            song.add(s.getSingerId());
        }
        System.out.println(song.size());
        Iterator<String> it = song.iterator();
        while(it.hasNext())
        {
            String currentId = it.next();
            HashMap<Integer,Integer> temp= new HashMap<>();
            for(int i = 20150301;i<20150332;i++)
            {
                temp.put(i,0);
            }
            for(int i = 20150401;i<20150431;i++)
            {
                temp.put(i,0);
            }
            for(int i = 20150501;i<20150532;i++)
            {
                temp.put(i,0);
            }
            for(int i = 20150601;i<20150631;i++)
            {
                temp.put(i,0);
            }
            for(int i = 20150701;i<20150732;i++)
            {
                temp.put(i,0);
            }
            for(int i = 20150801;i<20150831;i++)
            {
                temp.put(i,0);
            }
//            testingByDate.put(currentId,temp);
            predictingByDate.put(currentId, (HashMap<Integer, Integer>) temp.clone());
        }
        System.out.println("歌曲的数量"+song.size());

        for(Singer s:predict)//在这里是将每个歌手的有的数据进行更新
        {
            HashMap<Integer,Integer> temp = predictingByDate.get(s.getSingerId());
            temp.put(s.getDatetime(),s.getListenCount());
        }

        predict.clear();
        Iterator<String> it1 = song.iterator();

        while(it1.hasNext())
        {
            String currentId = it1.next();
            HashMap<Integer,Integer> currentIdMap = predictingByDate.get(currentId);
            for(int i = 20150301;i<20150332;i++)
            {
                predict.add(new Singer(currentId,currentIdMap.get(i),i));
            }
            for(int i = 20150401;i<20150431;i++)
            {
                predict.add(new Singer(currentId,currentIdMap.get(i),i));
            }
            for(int i = 20150501;i<20150532;i++)
            {
                predict.add(new Singer(currentId,currentIdMap.get(i),i));
            }
            for(int i = 20150601;i<20150631;i++)
            {
                predict.add(new Singer(currentId,currentIdMap.get(i),i));
            }
            for(int i = 20150701;i<20150732;i++)
            {
                predict.add(new Singer(currentId,currentIdMap.get(i),i));
            }
            for(int i = 20150801;i<20150831;i++)
            {
                predict.add(new Singer(currentId,currentIdMap.get(i),i));
            }

        }



        Collections.sort(predict);


    }

}


