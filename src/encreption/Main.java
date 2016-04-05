package encreption;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import encreption.AESSecurityUtil;

public class Main {
	private static int count1=0;
	private static int count2=0;
	public static void main(String args[]) throws Exception
	{
		File file = new File("/Users/chenwuji/Documents/StayDays");
		String rootpath = file.getAbsolutePath();
		String fileList[] = file.list();
		ArrayList<String> filelist2 = new ArrayList<String>();
		for(String ii:fileList)
		{
			if(ii.contains("part"))
			{
				filelist2.add(ii);
			}
		}
		for(String i:filelist2)
		{
			System.out.println(i);
			List<String> result = new ArrayList<String>();
			File f = new File(rootpath+"/"+i);
			if(f.isFile()&&f.exists())
			{

				InputStreamReader io = new InputStreamReader(new FileInputStream(f),"ASCII");
				BufferedReader rd = new BufferedReader(io);
				String line = null;
				while((line=rd.readLine())!=null)
				{
					count1++;
					System.out.println(line);
					line = line.substring(1,line.length()-1);

					result.add(line.split(",")[0]+"\t"+AESSecurityUtil.StartAES(line.split(",")[0])+"\t"+line.split(",")[1]);
				}
				io.close();
				count2+=result.size();
			}
			FileWriter wr = new FileWriter("/Users/chenwuji/Documents/output/StayDays.txt",true);
			for(String j:result)
			{
				wr.write(j+"\n");
			}
			wr.close();

		}
		System.out.println("count1:"+count1+"   count2 :"+count2);

	}

}
