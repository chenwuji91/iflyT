package localJudge;

import java.io.*;
import java.util.ArrayList;

/**
 * Created by chenwuji on 16/3/23.
 */
public class FileRead {

    public static ArrayList<Singer> loadPredicting()
    {
        ArrayList<Singer> predictList = FileRead.loadFile(Const.PredictFilePath);
        return predictList;

    }
    public static ArrayList<Singer> loadTesting()
    {
        ArrayList<Singer> testingList = FileRead.loadFile(Const.TestingFilePath);
        return testingList;

    }

    private static ArrayList<Singer> loadFile(String filePath) {
        ArrayList<Singer> singerList = new ArrayList<Singer>();
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
                    singerList.add(new Singer(lineS[0],Integer.valueOf(lineS[1]),Integer.valueOf(lineS[2])));
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
        if(singerList.size()!= Const.DayOfPredictDay*Const.singerCount) {
            System.out.println("Warning! Singer List Error! But go on judging");
        }
        return singerList;
    }
}
