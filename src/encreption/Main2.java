package encreption;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Main2 {
	private static int count1=0;
	private static int count2=0;
	public static void main(String args[]) throws Exception
	{
		File file = new File("/Users/chenwuji/Documents/待解密EMIC/");
		String rootpath = file.getAbsolutePath();
		String fileList[] = file.list();
		ArrayList<String> filelist2 = new ArrayList<String>();
		for(String ii:fileList)
		{
//			if(ii.contains("part"))
//			{
				filelist2.add(ii);
			//}
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
					//System.out.println(line);
					result.add(AESSecurityUtil.StartAES(line));
				}
				io.close();
				count2+=result.size();
			}
			FileWriter wr = new FileWriter("/Users/chenwuji/Documents/output/LocalEmic0414.txt",false);
			for(String j:result)
			{
				wr.write(j+"\n");
			}
			wr.close();

		}
		System.out.println("count1:"+count1+"   count2 :"+count2);

	}

}
