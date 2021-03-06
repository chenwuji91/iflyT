package localJudge;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

/**
 * Created by chenwuji on 16/3/23.
 */
public class Main {
    private ArrayList<Singer> predict;
    private ArrayList<Singer> testing;

    public static void main(String args[])
    {
        Main m =new Main();
        m.init();
        m.checkLegal();
        System.out.println("The F-value is : "+m.calculate());



    }
    private void init()
    {
        predict = FileRead.loadPredicting();
        testing = FileRead.loadTesting();
        Collections.sort(predict);
        Collections.sort(testing);

    }
    private void checkLegal()
    {
        if(predict.size()!=testing.size())
        {
            System.out.println("List Error Existing!!");
        }
        if(testing.size()!=Const.singerCount*Const.DayOfPredictDay)
        {
            System.out.println("Testing Set illegal, exiting...");
            System.exit(1);
        }
    }

    private double calculate()
    {
        double oneSinger[] = new double[Const.singerCount];
        double weight[] = new double[Const.singerCount];
        double fValue = 0;
        /* Calculate oneSinger value */
        Iterator<Singer> itPredict = predict.iterator();
        Iterator<Singer> itTest = testing.iterator();
        String firstTestId = testing.get(0).getSingerId();
        String firstPredictId = predict.get(0).getSingerId();
        int countDay = 0;int countSinger = 0;
        while(itTest.hasNext())
        {
            if(itPredict.hasNext())
            {
                Singer currentTest = itTest.next();
                Singer currentPredict = itPredict.next();
                if(currentTest.getSingerId()==firstTestId)
                {
                    if(currentTest.getSingerId() == currentPredict.getSingerId())
                    {
                        if(currentPredict.getDatetime()==currentTest.getDatetime())
                        {
                            oneSinger[countSinger] += (Math.pow((currentPredict.getListenCount()-currentTest.getListenCount())/currentTest.getListenCount(),2));
                            weight[countSinger] += currentTest.getListenCount();
                            countDay++;
                        }
                        else if(currentPredict.getDatetime()>currentTest.getDatetime())
                        {
                            System.out.println("Lost predict items, exiting...");
                            System.exit(1);
                        }
                        else
                        {
                            System.out.println("duplicate predict items, exiting...");
                            System.exit(1);
                        }
                    }
                    else{
                        System.out.println("Lost singer, exiting...");
                        System.exit(1);
                    }
                }
                else
                {
                    if(countDay!=Const.DayOfPredictDay)
                    {
                        System.out.println("Day Lost,continuing");
                    }
                    oneSinger[countSinger] = Math.sqrt(oneSinger[countSinger]/countDay);
                    weight[countSinger] = Math.sqrt(weight[countSinger]);
                    firstTestId = currentTest.getSingerId();
                    firstPredictId = currentPredict.getSingerId();
                    countSinger++;
                    countDay = 0;
                }
            }
        }

        for(int i = 0;i < Const.singerCount;i++)
        {
            fValue += (1-oneSinger[i])*weight[i];
        }

        return fValue;

    }
}
