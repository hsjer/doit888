package top.doe;

public class HelloDoe {
    public static void main(String[] args) throws InterruptedException {

        for(int i = 0; i < 1000; i++){
            System.out.println("hello doe");
            Thread.sleep(500);
        }
    }
}
