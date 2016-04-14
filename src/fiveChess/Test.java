package fiveChess;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

import fiveChess.Point;

public class Test {

	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		Point[] chesslistB = new Point[18*18];
		Point[] chesslistW = new Point[18*18];
		Point NextStep;

		int stepx,stepy;
		// TODO Auto-generated method stub
		ChessStrategy f=new ChessStrategy();//创建主框架
		f.setVisible(true);//显示主框架


		//System.in代表标准输入，就是键盘输入
		// Scanner sc = new Scanner(System.in);sc.hasNext()
		/**
		 *
		 */

		while(true){
			/**
			 * 首先黑子先落子
			 */
			NextStep=f.BlackNextStep(chesslistW);
			stepx=NextStep.getX();
			stepy=NextStep.getY();

			chesslistB=f.getChessBoard().BlackAddChess(stepx,stepy);

			/**
			 * 白子落
			 */

			NextStep=f.WhiteNextStep(chesslistB);
			stepx=NextStep.getX();
			stepy=NextStep.getY();
			chesslistW=f.getChessBoard().WhiteAddChess(stepx,stepy);

		}

	}



}
