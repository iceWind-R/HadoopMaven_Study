public class Test {
    public static void main(String[] args) {
        String str = "1\t0\t1\t2020/7/11 11:11:11\t837255\t6\t4+1+1=6\t小,双\t0\t0.00\t0.00\t1\t0.00\t1\t1";
        System.out.println(str.split("\t")[5]);
    }
}
