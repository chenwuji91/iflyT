package encreption;

/**
 * Created by chenwuji on 16/3/29.
 */
public class JiaMi {

    public static void main(String args[]) throws Exception {
        String s1 = null;
        s1 = AESSecurityUtil.StartAES_encode("460110244056103");
        System.out.println(s1);
        String s2 = AESSecurityUtil.StartAES("LSvDjaU7Ldz43x+6V6fhsQ==");
        System.out.println(s2);
    }
}
