package other.state_clear;

public class LockBenchmarkTest {
    public static void main(String[] args) {


        long count = 0;

        long start1 = System.nanoTime();
        for (long i = 0; i < 1000000000L; i++) {
            count++;
        }
        long end1 = System.nanoTime();

        System.out.println(end1-start1);



        long start2 = System.nanoTime();
        for (long i = 0; i < 1000000000L; i++) {
            synchronized ("lock") {
                count++;
            }
        }
        long end2 = System.nanoTime();

        System.out.println(end2-start2);
    }
}
