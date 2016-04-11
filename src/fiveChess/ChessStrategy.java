package fiveChess;
import java.awt.event.*;
import java.awt.*;

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
				chessNow[p.getX()][p.getY()] = 1;//当前位置有黑子
			}
			else if(p.getColor()==Color.white)
			{
				chessNow[p.getX()][p.getY()] =  2;//当前位置有白子
			}
		}
	}

	/**
	 *
	 * @param chesslist
	 * @return 所有可以落子的位置
	 * 搜索某一深度的一些空位，如果周围一定的范围内有棋子，认为这个位置是一个可行的位置
	 */
	public Point[] gen(Point[] chesslist, int deep){//默认设定deep为2

		Point[] availablePoint = new Point[18*18];
		int countAvailable = 0;
		refreshChess(chesslist);//刷新当前棋盘棋子状态
        neighbour(3);//第一次搜索棋盘上的可用区域,将可以放棋子的地方标记为3
        neighbour(4);//第二次搜索棋盘上面可以放棋子的区域,将可以放棋子的区域标记为4
		for(int i = 0;i< 18;i++)
		{
			for(int j = 0;j < 18 ; j++)
			{
//				if(chessNow[i][j]==0)//如果当前位置是空位置
//				for(int k = -deep;k<=deep;k++)//x偏移
//				{
//					for(int l = -deep;l<=deep;l++)//y偏移
//					{
//						if((k+l<=deep)&&(k+l>=-deep))
//                        {
//                            if(i+k<0||i+k>=18)//偏移超出边界
//                                break;
//                            if(j+l<0||j+l>=18)//偏移超出边界
//                                break;
//                            if(chessNow)
//                        }
//					}
//				}
			}
		}
		return chesslist;
	}
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

}
