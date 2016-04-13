package fiveChess;
import java.awt.event.*;
import java.awt.*;
import java.util.ArrayList;

import javax.swing.*;

import fiveChess.ChessBoard;
import fiveChess.Point;


public class ChessStrategy extends JFrame {

	/**
	 * @param args
	 */
	private ChessBoard chessBoard;

	private JPanel toolbar;
	private JButton startButton,backButton,exitButton;

	private JMenuBar menuBar;
	private JMenu sysMenu;
	private JMenuItem startMenuItem,exitMenuItem,backMenuItem;
	//重新开始，退出，和悔棋菜单项
	public ChessStrategy(){
		setTitle("单机版五子棋");//设置标题
		chessBoard=new ChessBoard();


		Container contentPane=getContentPane();
		contentPane.add(chessBoard);
		chessBoard.setOpaque(true);


		//创建和添加菜单
		menuBar =new JMenuBar();//初始化菜单栏
		sysMenu=new JMenu("系统");//初始化菜单
		//初始化菜单项
		startMenuItem=new JMenuItem("重新开始");
		exitMenuItem =new JMenuItem("退出");
		backMenuItem =new JMenuItem("悔棋");
		//将三个菜单项添加到菜单上
		sysMenu.add(startMenuItem);
		sysMenu.add(exitMenuItem);
		sysMenu.add(backMenuItem);
		//初始化按钮事件监听器内部类
		MyItemListener lis=new MyItemListener();
		//将三个菜单注册到事件监听器上
		this.startMenuItem.addActionListener(lis);
		backMenuItem.addActionListener(lis);
		exitMenuItem.addActionListener(lis);
		menuBar.add(sysMenu);//将系统菜单添加到菜单栏上
		setJMenuBar(menuBar);//将menuBar设置为菜单栏

		toolbar=new JPanel();//工具面板实例化
		//三个按钮初始化
		startButton=new JButton("重新开始");
		exitButton=new JButton("退出");
		backButton=new JButton("悔棋");
		//将工具面板按钮用FlowLayout布局
		toolbar.setLayout(new FlowLayout(FlowLayout.LEFT));
		//将三个按钮添加到工具面板
		toolbar.add(startButton);
		toolbar.add(exitButton);
		toolbar.add(backButton);
		//将三个按钮注册监听事件
		startButton.addActionListener(lis);
		exitButton.addActionListener(lis);
		backButton.addActionListener(lis);
		//将工具面板布局到界面”南方“也就是下方
		add(toolbar,BorderLayout.SOUTH);
		add(chessBoard);//将面板对象添加到窗体上
		//设置界面关闭事件
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		//setSize(800,800);
		pack();//自适应大小

	}
	private class MyItemListener implements ActionListener{
		public void actionPerformed(ActionEvent e){
			Object obj=e.getSource();//获得事件源
			if(obj==ChessStrategy.this.startMenuItem||obj==startButton){
				//重新开始
				//JFiveFrame.this内部类引用外部类
				System.out.println("重新开始");
				chessBoard.restartGame();
			}
			else if (obj==exitMenuItem||obj==exitButton)
				System.exit(0);
			else if (obj==backMenuItem||obj==backButton){
				System.out.println("悔棋...");
				chessBoard.goback();
			}
		}
	}

	public ChessBoard getChessBoard() {
		return chessBoard;
	}

	public Point WhiteNextStep(Point[] chessList){
		/**
		 * 白子策略
		 */
		return new Point(9,9,Color.BLACK);
		//return null;//修改返回下一步坐标点
	}

	public Point BlackNextStep(Point[] chesslistB) {//基于当前棋盘上面的子  寻找下一步走的方法  返回一个Point点
		// TODO Auto-generated method stub
		/**
		 * 黑子策略
		 */
		//return new Point(8,8,Color.BLACK);
		return null;//修改返回下一步坐标点
	}
}



class ComputerStrategy{
	private static int chessNow[][] = new int[18][18];
    /**
     * 尝试开始进行极大极小的走棋 作为方法调度的入口  接收策略的调度请求,返回走棋方案
     */
    public void maxAndMin(Point[] chesslist,int deep)
    {
        refreshChess(chesslist);
        gen(deep);
        ArrayList<Point> availablePoint = getAvailablePoint();
        int initalScore = evaluate();
        System.out.println("初始状态下面的得分是:"+initalScore);
        for(int i = 0;i < availablePoint.size();i++)
        {

        }


    }
    public int max()

	/**
	 * 将棋盘转换刷新为便于检索的形式
	 */
	public void refreshChess(Point[] chesslist)
	{
		for(int i=0;i<18;i++)
		{
			for(int j=0;j<18;j++)
			{
				chessNow[i][j] = 0;//表示当前位置为空
			}
		}
		for(Point p:chesslist)
		{
			if(p.getColor()==Color.black)
			{
				chessNow[p.getX()][p.getY()] = 1;//当前位置有黑子,在当前棋局下, 假设黑子为电脑
			}
			else if(p.getColor()==Color.white)
			{
				chessNow[p.getX()][p.getY()] =  2;//当前位置有白子
			}
		}
	}

	/**
	 *
	 * @param
	 * @return 所有可以落子的位置
	 * 搜索某一深度的一些空位，如果周围一定的范围内有棋子，认为这个位置是一个可行的位置
	 */
	public void gen(int deep){//默认设定deep为2
        if(deep!=2)
        {
            System.out.println("检查参数!");
            System.exit(1);
        }
		Point[] availablePoint = new Point[18*18];
//		int countAvailable = 0;
//		refreshChess(chesslist);//刷新当前棋盘棋子状态
        neighbour(3);//第一次搜索棋盘上的可用区域,将可以放棋子的地方标记为3
        neighbour(4);//第二次搜索棋盘上面可以放棋子的区域,将可以放棋子的区域标记为4
	}
    /**
     * 检查生成的点,并返回可以走的点的集合
     */
    public ArrayList<Point> getAvailablePoint() {//默认设定deep为2
        ArrayList<Point> availablePoint = new ArrayList<Point>();
        for (int i = 0; i < 18; i++)
            for (int j = 0; j < 18; j++) {
                if (chessNow[i][j] == 3) {
                    availablePoint.add(new Point(i, j, Color.blue));
                    chessNow[i][j] = 0;
                }
                if (chessNow[i][j] == 4) {
                    availablePoint.add(new Point(i, j, Color.blue));
                    chessNow[i][j] = 0;
                }

            }
        return availablePoint;
    }

    /**
     * 找邻居
     * @param status
     */
    private void neighbour(int status)
    {
        for(int i = 0;i< 18;i++)
        {
            for(int j = 0;j < 18 ; j++)
            {
                if(chessNow[i][j]==0)
                {
                    if(i-1>=0&&chessNow[i-1][j]!=0)
                        chessNow[i][j] = status;//  设置为指定的状态位
                    if(i+1<18&&chessNow[i+1][j]!=0)
                        chessNow[i][j] = status;//
                    if(j-1>=0&&chessNow[i][j-1]!=0)
                        chessNow[i][j] = status;//
                    if(j+1<18&&chessNow[i][j+1]!=0)
                        chessNow[i][j] = status;//
                }

            }
        }
    }

    /**
     *
     * @return 返回某个棋盘状态的评分值
     */
    public int evaluate(){
        int scoreComputer = 0;
        int scorePlayer = 0;
        ArrayList<int[]> allTheList = flat();//取得当前状态下面的所有数组集合
        for(int[] l:allTheList)
        {
            int countC = 0;
            int countP = 0;
            for(int i = 1;i<l.length-1;i++)
            {
                if(l[i]==0)
                {
                    if(l[i-1] == 1)
                    {
                        scoreComputer += returnScore(countC);
                        countC = 0;
                    }
                    else if(l[i-1] == 2) {
                        scorePlayer += returnScore(countP);
                        countP = 0;
                    }
                }
                else if(l[i]==1)
                {
                    if(l[i-1] ==1)
                    {
                        countC++;
                    }
                    else if(l[i-1]==2)
                    {
                        scorePlayer +=returnScore(countP-1);
                        countC = 0;
                    }
                    else if(l[i-1]==0)
                    {
                        countC = 1;
                    }
                }
                else if(l[i]==2)
                {
                    if(l[i-1] == 2)
                    {
                        countP++;
                    }
                    else if(l[i-1] == 1)
                    {
                        scoreComputer += returnScore(countC - 1);
                    }
                    else if(l[i-1] == 0)
                    {
                        countP = 1;
                    }
                }
            }
            //处理末尾元素
            if(l[l.length-1]==1)
            {
                if(l[l.length-2] == 2)
                {
                    scorePlayer += returnScore(countP -1);
                }
                if(l[l.length-2] == 1)
                {
                    scoreComputer +=returnScore(countC);
                }
            }
            if(l[l.length-1]==2)
            {
                if(l[l.length-2] == 1){
                    scoreComputer += returnScore(countC-1);
                }
                if(l[l.length-2] == 2)
                {
                    scorePlayer += returnScore(countP);
                }
            }
            if(l[l.length-1]==0)
            {
                scoreComputer += returnScore(countC);
                scorePlayer += returnScore(countP);
            }

        }
        return scoreComputer - scorePlayer;
    }

    /**
     * 根据个数返回当前棋子组合的分数
     * @param count
     * @return
     */
    private int returnScore(int count)
    {
        switch(count)
        {
            case 0 : return 0;
            case 1 : return 10;
            case 2 : return 100;
            case 3 : return 1000;
            case 4 : return 10000;
            case 5 : return 100000;
        }
        return 0;
    }


    /**
     *
     * @return  将原始的状态数组返回成一个 一维数组的形式  这个一位数组包含了所有五子棋允许的相关状态 返回值用ArrayList保存
     */
    private ArrayList<int[]> flat()
    {
        ArrayList<int[]> singleList = new ArrayList<int[]>();
        for(int i = 0;i< 18;i++)//添加横的行
            singleList.add(chessNow[i]);
        for(int i = 0;i<18;i++)//添加竖的列
        {
            int[] temp = new int[18];
            for(int j=0;j<18;j++)
            {
                temp[j] = chessNow[j][i];
            }
            singleList.add(temp);
        }
        for(int i = 0;i < 14;i++)//添加向下方斜的数字
        {
            int[] temp = new int[18-i];
            int[] temp2 = new int[18-i];
            for(int j=0;j<18-i;j++)
            {
                temp[j]= chessNow[j][j+i];
                temp2[j] = chessNow[j+i][j];
            }
            singleList.add(temp);
            singleList.add(temp2);
        }
        for(int i=0;i<14;i++)
        {
            int temp[] = new int[18-i];
            int temp2[] = new int[18-i];
            for(int j = 17;j > 4;j--)
            {
                temp[j] = chessNow[17-j+i][j];
                temp2[j] = chessNow[17-j][j-i];

            }
            singleList.add(temp);
            singleList.add(temp2);
        }
        return singleList;
    }

}
