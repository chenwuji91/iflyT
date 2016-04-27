package suzhouRoadRecovery;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Stack;


public class recordrecover {
	/*
	 * 步骤：
	 * 1.读入整个txt文档；
	 * 2.整个文件处理过程是从早上8点开始；
	 * 3.这个时候开始取第一行数据，把此数据作为起点也是终点，分别找出最近的路口；
	 * 4.按照DFS算法，寻找出最短路径；
	 * 5.找出最短路径之后，计算两条记录之间相差的时间是4s的多少倍；
	 * 6.然后计算最短路径的长度，除以上面的倍数，即可得到这段时间内如果匀速行驶，4s一条记录的话，每4s我到了什么地方；
	 * 7.紧接着，按照4s一条记录的生成，此时就只考虑路口到路口的距离了，不考虑实际到的还有首尾的路段到路口的距离；
	 * 8.*/
	public static void main(String []args){
		recordrecover rec = new recordrecover("E:\\data\\intersection_newid.txt","E:\\data\\roadnet_newid.txt","E:\\data\\taxiidmap.txt");
		rec.Readfile();
	}
	static Stack s = new Stack();
	static int Car_Num = 3998;
	static public int LuKou_Num = 1403;
	static public int Road_Num = 4300;
	static public int Days = 31;
	static double [][]LuKou = new double[LuKou_Num][2];
	static int [][]Net = new int[LuKou_Num][];

	static int []Taxiidmap = new int[3998];
	ArrayList<ArrayList<Integer>> allpath =  new ArrayList<ArrayList<Integer>>();
	ArrayList<Integer> bestpath = new ArrayList<Integer>();

	//String IDmap = "";
	recordrecover(String path1,String path2,String IDmap){
		File file1 = new File(path1);
		File file2 = new File(path2);
		String text1 = ReadToString(file1);
		String []line1 = text1.split("\n");
		String text2 = ReadToString(file2);
		String []line2 = text2.split("\n");
		for(int i = 0;i < LuKou_Num;i++){
			//System.out.println(line1[i]);
			String []strArray = line1[i].split(",");
			LuKou[i][0] = Double.parseDouble(strArray[2]);
			LuKou[i][1] = Double.parseDouble(strArray[3]);
		}
		for(int i = 0;i < LuKou_Num;i++){
			//System.out.println(line2[i]);
			String []strArray = line2[i].split(" |: |\r|:");
			Net[i] = new int[strArray.length];
			for(int j = 0;j < strArray.length;j++){
				Net[i][j] = Integer.parseInt(strArray[j]);
			}
		}


		File file = new File(IDmap);
		String text = ReadToString(file);
		String []line11 = text.split("\n");
		for(int i = 0;i < line11.length;i++){
			//System.out.println(line11[i]);
			String []strArray = line11[i].split(":|\r");
			Taxiidmap[i] = Integer.parseInt(strArray[1]);
		}
	}

	static String ReadToString(File file){
		Long filelength = file.length();     //获取文件长度
		byte[] filecontent = new byte[filelength.intValue()];
		try {
			FileInputStream in = new FileInputStream(file);
			in.read(filecontent);
			in.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return new String(filecontent);//返回文件内容,默认编码
	}


	private static double EARTH_RADIUS = 6378.137;
	private static double rad(double d)
	{
		return d * Math.PI / 180.0;
	}
	public static double GetDistance(double lat1, double lng1, double lat2, double lng2)
	{
		double radLat1 = rad(lat1);
		double radLat2 = rad(lat2);
		double a = radLat1 - radLat2;
		double b = rad(lng1) - rad(lng2);
		double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a/2),2) +
				Math.cos(radLat1)*Math.cos(radLat2)*Math.pow(Math.sin(b/2),2)));
		s = s * EARTH_RADIUS;
		s = Math.round(s * 10000) / 10000;
		return s;
	}

	static int Nearest_LuKou(double x,double y){
		int Index = 0;
		double Nearest_Distance = 999999999;
		for(int i = 0;i < LuKou_Num;i++){
			double dis = Math.acos(Math.cos(y)*Math.cos(LuKou[i][1])*Math.cos(x-LuKou[i][0])+Math.sin(y)*Math.sin(LuKou[i][1]));
			if(dis < Nearest_Distance){
				Nearest_Distance = dis;
				Index = i;
			}
		}
		return Index;
	}

	void Readfile(){
		String filepath;
		try{
			System.out.println("ssdfksdjfkdsjfklds");
			for(int i = 0;i < 1;i++){//days
				for(int j = 0;j < 1;j++){//carnum
					if(i + 1 < 10){
						filepath = "C:\\Users\\Administrator\\Desktop\\suzhou\\"+"2012030"+String.valueOf(i+1)+"\\"+String.valueOf(Taxiidmap[j])+".txt";
					}
					else{
						filepath = "C:\\Users\\Administrator\\Desktop\\suzhou\\"+"201203"+String.valueOf(i+1)+"\\"+String.valueOf(Taxiidmap[j])+".txt";
					}
					filepath = "C:\\Users\\Administrator\\Desktop\\suzhou\\10333.txt";
					String encoding = "GBK";
					File file = new File(filepath);
					//System.out.println(filepath);
					//System.out.println(file.isFile()+"    "+file.exists());
					if(file.isFile() && file.exists()){
						//System.out.println(filepath);
						InputStreamReader read;
						read = new InputStreamReader(new FileInputStream(file),encoding);
						BufferedReader bufferedReader = new BufferedReader(read);
						String lineTxt = bufferedReader.readLine();
						String []strArray = lineTxt.split(",");
						Timestamp ts = new Timestamp(System.currentTimeMillis());
						ts = Timestamp.valueOf(strArray[5]);
						boolean torf = Integer.valueOf(strArray[6])==1?true:false;
						Record begin = new Record(Integer.parseInt(strArray[0]),Double.parseDouble(strArray[1]),Double.parseDouble(strArray[2]),Integer.parseInt(strArray[3]),Integer.parseInt(strArray[4]),ts,torf);
						lineTxt = bufferedReader.readLine();

						String outpath;
						if(i + 1 < 10)
						{outpath = "C:\\Users\\Administrator\\Desktop\\suzhourec\\"+"2012030"+String.valueOf(i + 1)+"\\"+String.valueOf(Taxiidmap[j])+".txt";}
						else{outpath = "C:\\Users\\Administrator\\Desktop\\suzhourec\\"+"201203"+String.valueOf(i + 1)+"\\"+String.valueOf(Taxiidmap[j])+".txt";}

						FileOutputStream fos = new FileOutputStream(outpath);
						PrintStream p = new PrintStream(fos);
						while((lineTxt = bufferedReader.readLine()) != null){
							String []strArray1 = lineTxt.split(",");
							//	System.out.println(lineTxt);
							Timestamp ts1 = new Timestamp(System.currentTimeMillis());
							ts1 = Timestamp.valueOf(strArray1[5]);
							System.out.println("Strarray1[6]:"+strArray1[6]);

							boolean torf1;
							if(Integer.valueOf(strArray1[6])==1){torf1 = true;}else{torf1 = false;}
							System.out.println("true or false:"+torf1);
							Record end = new Record(Integer.parseInt(strArray1[0]),Double.parseDouble(strArray1[1]),Double.parseDouble(strArray1[2]),Integer.parseInt(strArray1[3]),Integer.parseInt(strArray1[4]),ts1,torf1);
							//先找到起点终点的最近路口
							int startnode = Nearest_LuKou(begin.x,begin.y);
							int endnode = Nearest_LuKou(end.x,end.y);
							System.out.println(startnode + "   " + String.valueOf(begin.time));
							System.out.println(endnode + "    " + String.valueOf(end.time));
							//先找到最短路径
							int nownode = startnode;
							int result = BFS(startnode,endnode);
						/*
						 * 此时，bestpath为最短路径
						 * 首先根据两点时差，判断以4s为间隔能够产生多少个恢复的记录
						 * 如果两点时差小于4s，则向恢复txt中直接写入这两条记录
						 * 如果不是的话，那么就需要按照时间间隔来进行记录产生
						 * 产生的时候，考虑到行车方向与正北角度，此角度直接考虑该路段的角度
						 * 速度的话，则考虑按照匀速产生，载客情况，按照之前与老师讨论结果*/


							if(result == -1)
							{
								System.out.println("两条记录之间原地未动！");
								System.out.println(begin.id+"  "+begin.x+"  "+begin.y+"  "+0+"  "+begin.angle+"  "+begin.time+"  "+begin.oc+"\r\n");
								p.print(begin.id+"  "+begin.x+"  "+begin.y+"  "+0+"  "+begin.angle+"  "+begin.time+"  "+begin.oc+"\r\n");
							}else{
								if(result == -2){
									System.out.println("两条记录之间距离太远，无法还原！");
									System.out.println(begin.id+"  "+begin.x+"  "+begin.y+"  "+0+"  "+begin.angle+"  "+begin.time+"  "+begin.oc+"\r\n");
									p.print(begin.id+"  "+begin.x+"  "+begin.y+"  "+0+"  "+begin.angle+"  "+begin.time+"  "+begin.oc+"\r\n");
								}
								else{
									System.out.print("BESTPATH:");
									for(int dd = 0;dd < bestpath.size();dd++){System.out.print(bestpath.get(dd)+" ");}
									System.out.println();
									long time = end.time.getTime() - begin.time.getTime();
									int allsize = (int)(time/4000);
									int roadsize = allsize/(bestpath.size()-1);
									int ser = allsize/roadsize;
									System.out.println("time: "+time+" allsize: "+allsize+" roadsize: "+roadsize+" ser: "+ser);

									p.print(begin.id+"  "+begin.x+"  "+begin.y+"  "+0+"  "+begin.angle+"  "+begin.time+"  "+begin.oc+"\r\n");

									Timestamp tsmid = new Timestamp(System.currentTimeMillis());
									tsmid = begin.time;

									for(int rs = 0;rs < bestpath.size() - 1;rs++){
										for(int see = 1;see <= roadsize-1;see++){
											tsmid.setTime(tsmid.getTime()+4000);
											p.print(begin.id+"  "+see*(LuKou[bestpath.get(rs)][0]+LuKou[bestpath.get(rs)][0])/ser+"  "
													+see*(LuKou[bestpath.get(rs)][1]+LuKou[bestpath.get(rs)][1])/ser+"  "
													+getAngle(LuKou[bestpath.get(rs)][0],LuKou[bestpath.get(rs)][1],LuKou[bestpath.get(rs+1)][0],
													LuKou[bestpath.get(rs+1)][1])+"  "
													+tsmid+"  "+begin.oc+"\r\n");
										}
										tsmid.setTime(tsmid.getTime()+4000);
										if(rs != bestpath.size()-2){
											p.print(begin.id+"  "+LuKou[bestpath.get(rs+1)][0]+"  "
													+LuKou[bestpath.get(rs+1)][1]+"  "
													+getAngle(LuKou[bestpath.get(rs)][0],LuKou[bestpath.get(rs)][1],LuKou[bestpath.get(rs+1)][0],
													LuKou[bestpath.get(rs+1)][1])+"  "
													+tsmid+"  "+begin.oc+"\r\n");}

									}
								}
							}
							begin = end;
							bufferedReader.readLine();
							s.clear();
							bestpath.clear();
						}

						System.out.println(begin.id+"  "+begin.x+"  "+begin.y+"  "+0+"  "+begin.angle+"  "+begin.time+"  "+begin.oc+"\r\n");
						p.print(begin.id+"  "+begin.x+"  "+begin.y+"  "+0+"  "+begin.angle+"  "+begin.time+"  "+begin.oc+"\r\n");

					}

					//
				}
			}
		}catch(Exception e){e.printStackTrace();}
	}

	int BFS(int startnode,int endnode){
		if(startnode == endnode){return -1;}
		int u;
		int deep = 0;
		LinkedList queue = new LinkedList();
		boolean []isVisited = new boolean[LuKou_Num];
		for(int i = 0;i < LuKou_Num;i++){isVisited[i] = false;}
		int []previous = new int[LuKou_Num];
		for(int i = 0;i < LuKou_Num;i++){previous[i] = -2;}
		previous[startnode] = -1;
		isVisited[startnode] = true;
		queue.addLast(startnode);
		while(!queue.isEmpty()){
			deep ++;
			if(deep > 10000){return -2;}
			u = (Integer)queue.removeFirst();
			//System.out.print(u+": ");
			for(int j = 1;j < Net[u].length;j++){
				if(!isVisited[Net[u][j]]){
					if(Net[u][j] == endnode){//如果找到了终点，那么根据previous数组依次寻找路径
						previous[Net[u][j]] = u;
						int prenode = Net[u][j];
						s.push(prenode);
						int  index = 0;
						while(previous[prenode] >-2 && (index) < 10){
							index ++;
							System.out.println(prenode+"  "+previous[prenode]);
							if(previous[prenode] == -1){
								// s.push(previous[prenode]);
								 /*
								  * 此时，已经找到了最短路径，最短路径的路口保存在栈s中*/
								System.out.println("FOUNDED!!！！");
								while(!s.empty()){
									bestpath.add((Integer) s.pop());
								}
								return 0;
							}else{
								s.push(previous[prenode]);
								prenode = previous[prenode];
							}
						}
					}else{//如果没有找到终点
						queue.add(Net[u][j]);
						previous[Net[u][j]] = u;
						isVisited[Net[u][j]] = true;
					}
				}
			}
			System.out.println();
		}
		return -2;

	}

	public static double getAngle(double x1,double y1,double x2,double y2){
		MyLatLng A=new MyLatLng(x1,y1);
		MyLatLng B=new MyLatLng(x2,y2);
		double dx=(B.m_RadLo-A.m_RadLo)*A.Ed;
		double dy=(B.m_RadLa-A.m_RadLa)*A.Ec;
		double angle=0.0;
		angle=Math.atan(Math.abs(dx/dy))*180./Math.PI;
		double dLo=B.m_Longitude-A.m_Longitude;
		double dLa=B.m_Latitude-A.m_Latitude;
		if(dLo>0&&dLa<=0){
			angle=(90.-angle)+90;
		}
		else if(dLo<=0&&dLa<0){
			angle=angle+180.;
		}else if(dLo<0&&dLa>=0){
			angle= (90.-angle)+270;
		}
		return angle;
	}

	private int findnextnode(HashSet path,ArrayList<Integer>onepath,int node1,int node2,int deep) {
		// TODO Auto-generated method stub
		for(int i = 0;i < Net[node1].length;i++){
			if(Net[node1][i] == node2){
				if(i == Net[node1].length - 1){
					//if()
				}
				else{
					return Net[node1][i+1];
				}
			}
		}
		return -1;
	}

}


class MyLatLng {
	final static double Rc=6378137;
	final static double Rj=6356725;
	double m_LoDeg,m_LoMin,m_LoSec;
	double m_LaDeg,m_LaMin,m_LaSec;
	double m_Longitude,m_Latitude;
	double m_RadLo,m_RadLa;
	double Ec;
	double Ed;
	public MyLatLng(double longitude,double latitude){
		m_LoDeg=(int)longitude;
		m_LoMin=(int)((longitude-m_LoDeg)*60);
		m_LoSec=(longitude-m_LoDeg-m_LoMin/60.)*3600;

		m_LaDeg=(int)latitude;
		m_LaMin=(int)((latitude-m_LaDeg)*60);
		m_LaSec=(latitude-m_LaDeg-m_LaMin/60.)*3600;

		m_Longitude=longitude;
		m_Latitude=latitude;
		m_RadLo=longitude*Math.PI/180.;
		m_RadLa=latitude*Math.PI/180.;
		Ec=Rj+(Rc-Rj)*(90.-m_Latitude)/90.;
		Ed=Ec*Math.cos(m_RadLa);
	}
}
