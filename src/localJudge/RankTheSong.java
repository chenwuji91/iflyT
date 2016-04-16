package localJudge;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;

/**
 * Created by chenwuji on 16/4/16.
 */
public class RankTheSong {
    private ArrayList<Singer> predict;
    private ArrayList<Singer> testing;
    public static void main(String args[])
    {
        RankTheSong m =new RankTheSong();
        m.init();

    }

    private void init()
    {
        predict = FileRead.loadPredicting();
        testing = FileRead.loadTesting();
        Collections.sort(predict);
        Collections.sort(testing);
        System.out.println("完成初始化");

    }

    private static void fillData()
    {
        ArrayList<Singer> songList = FileRead.loadSongList();
        HashSet<String> song = new HashSet<String>();
        for(Singer s:songList)
        {
            song.add(s.getSingerId());
        }
        for()

    }

}


