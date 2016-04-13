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
    private void repair()
    {
        System.out.println("数据裁剪修复,请检查数据源");
//        for(int i = 0;i<testing.size();i++)
//        {
//            while(predict.get(i).getDatetime()>testing.get(i).getDatetime())
//            {
//                System.out.println("删除记录"+predict.get(i).getSingerId()+" "+predict.get(i).getDatetime());
//                predict.remove(i);
//            }
//        }
//        for(Singer s:predict)
//        {
//            if(s.getDatetime()==20150831||s.getDatetime()==20150731)
//                predict.remove(s);
//        }
        for(int i = 0;i<predict.size();i++)
        {
            if(predict.get(i).getDatetime()==20150831)
                predict.remove(i);
        }
    }


    private void init()
    {
        predict = FileRead.loadPredicting();
        testing = FileRead.loadTesting();
        Collections.sort(predict);
        Collections.sort(testing);
        System.out.println("完成初始化");

    }
    private void checkLegal()
    {
        if(predict.size()!=testing.size())
        {
            System.out.println("List Error Existing!!Error code:"+predict.size()+","+testing.size());
            repair();
            System.out.println("Now:"+predict.size()+","+testing.size());
        }
        if(testing.size()!=Const.singerCount*Const.DayOfPredictDay)
        {
            System.out.println("Testing Set illegal, exiting...");
            System.exit(1);
        }
        System.out.println("测试数据集-训练数据及基本匹配通过");
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

                if(countDay==Const.DayOfPredictDay)
                {
                    if(currentTest.getSingerId().equals(firstTestId)&&currentPredict.getSingerId().equals(firstPredictId))
                    {
                        System.out.println("Calculate Error is coming,exiting..."+" "+countDay+" "+Const.DayOfPredictDay);
                        System.exit(1);
                    }
                    oneSinger[countSinger] = Math.sqrt(oneSinger[countSinger]/countDay);
                    weight[countSinger] = Math.sqrt(weight[countSinger]);
                    firstTestId = currentTest.getSingerId();
                    firstPredictId = currentPredict.getSingerId();
                    countSinger++;
                    countDay = 0;
                }

                if(currentTest.getSingerId().equals(firstTestId)&&currentPredict.getSingerId().equals(firstPredictId))
                {
                    if(currentTest.getSingerId().equals(currentPredict.getSingerId()) )
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
                        System.out.println("Lost singer, exiting..."+currentTest.getSingerId()+" "+currentPredict.getSingerId());
                        System.exit(1);
                    }
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
