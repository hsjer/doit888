package top.doe.javase.tree;

import java.util.Arrays;
import java.util.Collections;

public class TestDemo {
    public static void main(String[] args) {


        //    select  id,name from (select * from t ) o where id>2

        FileScan fileScan = new FileScan("file_scan", "/user/hive/warehouse/t", "parquet", Collections.emptyList());

        Project project1 = new Project("project", Arrays.asList("id", "name", "age"), Collections.singletonList(fileScan));

        Filter filter = new Filter("filter", "id>2", Collections.singletonList(project1));

        Project project2 = new Project("project", Arrays.asList("id", "name"), Collections.singletonList(filter));


        explain(project2);

    }


    public static void explain(TreeNode treeNode) {
        System.out.println(treeNode);

        TreeNode treeNode1 = treeNode.children().get(0);
        System.out.println("|+ " + treeNode1);

        TreeNode treeNode2 = treeNode1.children().get(0);
        System.out.println("  |+ " + treeNode2);

        TreeNode leafNode = treeNode2.children().get(0);
        System.out.println("     |+ " + leafNode);


    }

}
