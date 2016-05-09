package suzhouRoadRecovery;

import java.sql.Timestamp;

public class Record {
	 int id;
	 double x;
	 double y;
	 int speed;
	 int angle;
	 Timestamp time;
	 boolean oc;
	
	Record(int id,double x,double y,int speed,int angle,Timestamp time,boolean oc){
		this.id = id;
		this.x = x;
		this.y = y;
		this.speed = speed;
		this.angle = angle;
		this.time = time;
		this.oc = oc;


	}
	
	
	
	
}
