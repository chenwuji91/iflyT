package RoadMatch;

import com.sun.tools.hat.internal.parser.ReadBuffer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

/**
 * Created by chenwuji on 16/4/14.
 */
public class RoadTestDataTongyihua {
    public static void main(String args[]) throws Exception
    {
        File f = new File("/Users/chenwuji/Desktop/sequenceDone/asdOut04143.txt");
        InputStreamReader io = new InputStreamReader(new FileInputStream(f));
        BufferedReader rd = new BufferedReader(io);
        String line = null;
        while((line = rd.readLine()) != null)
        {
            String[] allData = line.split("\\)\\),\\(\\(");
            System.out.println(allData.length);
            for(String j:allData)
                System.out.println(j);
        }




    }
}
