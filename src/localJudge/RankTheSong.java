package localJudge;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by chenwuji on 16/4/16.
 */
public class RankTheSong {
    private ArrayList<Singer> predict;
    private ArrayList<Singer> testing;
    private ArrayList<Singer> songList;

    public static void main(String args[]) throws IOException {
        RankTheSong m =new RankTheSong();
        m.init();
        m.fillData();
        m.rank();
    }

    public void rank() throws IOException {
        if(predict.size()!=testing.size())
        {
            System.out.println("什么鬼");
            System.exit(1);
        }
//        if()
        FileWriter wr = new FileWriter(Const.OutputPath+"RankingResult.txt",true);
        for(int i=0;i<Const.songCount;i++)
        {
            double score = 0;
            for(int j = 0;j < Const.DayOfPredictDay;j++)
            {
                if(!(predict.get(i*Const.DayOfPredictDay+j).getSingerId().equals(testing.get(i*Const.DayOfPredictDay+j).getSingerId())&&predict.get(i*Const.DayOfPredictDay+j).getDatetime()==testing.get(i*Const.DayOfPredictDay+j).getDatetime()))
                {
                    System.out.println("fetal Error!");
                    System.exit(1);
                }
                score = score + Math.pow(predict.get(i*Const.DayOfPredictDay+j).getListenCount()-testing.get(i*Const.DayOfPredictDay+j).getListenCount(),2);
            }
            score = Math.sqrt(score/Const.DayOfPredictDay);
            wr.write(predict.get(i*Const.DayOfPredictDay).getSingerId()+","+score+"\n");
        }
        wr.close();
    }

    private void init()
    {
        predict = FileRead.loadPredictingBySong();//加载训练数据集
        testing = FileRead.loadTestingBySong();//加载测试数据集
        songList = FileRead.loadSongList();

        System.out.println("完成数据加载");
    }

    private void fillData()
    {
        HashMap<String,HashMap<Integer,Integer>> testingByDate = new HashMap<>();
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
            for(int i = 20150701;i<20150716;i++)
            {
                temp.put(i,0);
            }
            testingByDate.put(currentId,temp);
            predictingByDate.put(currentId, (HashMap<Integer, Integer>) temp.clone());
        }
        System.out.println("歌曲的数量"+song.size());

        for(Singer s:predict)
        {
            HashMap<Integer,Integer> temp = predictingByDate.get(s.getSingerId());
            temp.put(s.getDatetime(),s.getListenCount());
        }
        for(Singer s:testing)
        {
            HashMap<Integer,Integer> temp = testingByDate.get(s.getSingerId());
            temp.put(s.getDatetime(),s.getListenCount());
        }


        predict.clear();
        Iterator<String> it1 = song.iterator();
        while(it1.hasNext())
        {
            String currentId = it1.next();
            HashMap<Integer,Integer> currentIdMap = predictingByDate.get(currentId);
            for(int i = 20150716;i<20150732;i++)
            {
                //System.out.println(currentIdMap.size());
                predict.add(new Singer(currentId,currentIdMap.get(i),i));
            }
            for(int i = 20150801;i<20150831;i++)
            {
                predict.add(new Singer(currentId,currentIdMap.get(i),i));
            }
        }

        testing.clear();
        Iterator<String> it2 = song.iterator();
        while(it2.hasNext())
        {
            String currentId = it2.next();
            HashMap<Integer,Integer> currentIdMap = testingByDate.get(currentId);
            for(int i = 20150716;i<20150732;i++)
            {
                //System.out.println(currentIdMap.size());
                testing.add(new Singer(currentId,currentIdMap.get(i),i));
            }
            for(int i = 20150801;i<20150831;i++)
            {
                testing.add(new Singer(currentId,currentIdMap.get(i),i));
            }
        }

        Collections.sort(predict);
        Collections.sort(testing);

    }

}


