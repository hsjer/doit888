package top.doe.md5;


import org.apache.commons.codec.digest.Md5Crypt;

public class TestMd5 {
    public static void main(String[] args) {

        String salt = "$1$abcdefgh";
        String s = Md5Crypt.md5Crypt("1".getBytes(),salt);
        System.out.println(s);


        String s2 = Md5Crypt.md5Crypt("2".getBytes(),salt);
        System.out.println(s2);

    }
}
