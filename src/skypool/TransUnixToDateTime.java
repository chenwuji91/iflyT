package skypool;



public class TransUnixToDateTime {

    public static String translate(Object time)
    {
        Long timestamp = Long.parseLong(time.toString())*1000;
        String date = new java.text.SimpleDateFormat("yyyyMMddHH").format(new java.util.Date(timestamp));
        return date;
    }


}
