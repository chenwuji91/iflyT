package localJudge;

/**
 * Created by chenwuji on 16/3/23.
 */
public class Singer implements Comparable{
    private String singerId;
    private int listenCount;
    private int datetime;

    public Singer() {
    }

    public Singer(String singerId, int listenCount, int datetime) {
        this.singerId = singerId;
        this.listenCount = listenCount;
        this.datetime = datetime;
    }

    public String getSingerId() {
        return singerId;
    }

    public int getListenCount() {
        return listenCount;
    }

    public int getDatetime() {
        return datetime;
    }

    public void setSingerId(String singerId) {
        this.singerId = singerId;
    }

    public void setListenCount(int listenCount) {
        this.listenCount = listenCount;
    }

    public void setDatetime(int datetime) {
        this.datetime = datetime;
    }

    @Override
    public int compareTo(Object o) {
        if(!(o instanceof Singer))
        {
            throw new java.lang.IllegalArgumentException("参数错误");
        }
        if((this.singerId.compareTo(((Singer) o).getSingerId()) > 0))
        {
            return 1;
        }
        else if((this.singerId.compareTo(((Singer) o).getSingerId()) < 0))
        {
            return -1;
        }
        else
        {
            if(this.datetime > ((Singer) o).getDatetime())
            {
                return 1;
            }
            else if(this.datetime < ((Singer) o).getDatetime())
            {
                return -1;
            }
            else
            {
                System.out.println("Warning! Duplicate items exists!");
                return 0;
            }

        }
    }
}
