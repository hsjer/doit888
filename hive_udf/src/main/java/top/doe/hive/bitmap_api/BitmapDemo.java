package top.doe.hive.bitmap_api;

import org.roaringbitmap.RoaringBitmap;

public class BitmapDemo {
    public static void main(String[] args) {

        // 构造一个初始的全为0的bitmap
        RoaringBitmap bitmap_1 = RoaringBitmap.bitmapOf();

        // 往bitmap中添加元素
        bitmap_1.add(1);
        bitmap_1.add(3);
        bitmap_1.add(5);

        System.out.println(bitmap_1.toString());
        System.out.println(bitmap_1.toString().replaceAll("\\{","\\[").replaceAll("\\}","\\]"));

        // 取bitmap的基数（bit 1的个数）
        System.out.println(bitmap_1.getCardinality());



        // 构造一个初始的全为0的bitmap
        RoaringBitmap bitmap_2 = RoaringBitmap.bitmapOf();
        bitmap_2.add(2);
        bitmap_2.add(3);
        bitmap_2.add(4);
        bitmap_2.add(5);


        // 把两个bitmap_2合并到bitmap_1中
        bitmap_1.or(bitmap_2);

        System.out.println(bitmap_1.getCardinality());


        // 判断bitmap中是否存在指定的元素
        System.out.println( bitmap_1.contains(3)  );
        System.out.println( bitmap_1.contains(4)  );
        System.out.println( bitmap_1.contains(8)  );



    }
}
