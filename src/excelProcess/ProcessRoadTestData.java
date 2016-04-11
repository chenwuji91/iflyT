package excelProcess;

import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

/**
 * Created by chenwuji on 16/4/7.
 */
public class ProcessRoadTestData {

    public static final String path1 = "/Users/chenwuji/Downloads/芜湖市政府/";
    public static final String path2 = "/Users/chenwuji/Downloads/安师大南校区/";
    public static final String outputPath = "/Users/chenwuji/Documents/RoadTest/";
    public static final String AESEmic = "QoUs940IZkYpoNdQlSEqkw==";
    public static final String Other = "Service_Req,4G";
    public static long timeDeviation = 0;

    public static ArrayList<String> fileList() throws IOException{
        File file = new File(path1);
        String rootpath = file.getAbsolutePath();
        String fileList[] = file.list();
        ArrayList<String> filelist2 = new ArrayList<String>();
        for(String ii:fileList)
        {
            if(ii.contains("xlsx")&&(!ii.contains("~")))
            {
                filelist2.add(ii);
            }
        }
        return filelist2;

    }

    public static void readExcel(String filename) throws Exception
    {
        //得到Excel工作簿对象
        XSSFWorkbook wb = new XSSFWorkbook(path1+filename);
        Iterator it = wb.iterator();
        while(it.hasNext())
        {
            timeDeviation++;
            XSSFSheet sheet = (XSSFSheet)it.next();
            String sheetName = sheet.getSheetName();
       //     System.out.println("输出1"+);
            for(int i = 4; i< sheet.getLastRowNum();i++)
            {

                XSSFRow row = sheet.getRow(i);
                if(row == null)
                    continue;
                if(row.getLastCellNum()<5)
                    continue;
                XSSFCell cellID = row.getCell((short) 5);
                int cellType = cellID.getCellType();
                if(cellType==0)
                {
                    String xiaoQuID = cellID.getRawValue();
                    XSSFCell cellDate = row.getCell((short) 1);
                    if(cellDate.getCellType()!=0)
                        continue;
                    Date cellData1 = cellDate.getDateCellValue();
                    if(cellData1.getYear()<2000)
                        cellData1 = row.getCell((short) 0).getDateCellValue();
                    Date datediviation = new Date(cellData1.getTime()+ timeDeviation*3600000L*24L);
                    SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
                    //String date = df.format(cellData1);
                    String date = df.format(datediviation);

                    System.out.println(date + " "+ xiaoQuID);
                    int xiaoquProcessed = Integer.valueOf(xiaoQuID.substring(0,6))*256+Integer.valueOf(xiaoQuID.substring(6,8));
                    writeToFile(outputPath+filename+"_"+sheetName,date+","+","+xiaoquProcessed+","+AESEmic+","+Other);
                }

            }

        }



    }
    private static void writeToFile(String path,String data) throws Exception
    {
        FileWriter wr = new FileWriter(path+".csv",true);
        wr.write(data+"\n");
        wr.close();
    }

    public static void main(String args[]) throws Exception {
        ArrayList<String> list= fileList();
        System.out.println(list);
        for(String i:list)
        {
            readExcel(i);
        }
    }
}
