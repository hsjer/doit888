package top.doe.javase.tree;

import lombok.*;

import java.util.List;


@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class FileScan implements TreeNode{
    String nodeName = "fileScan";
    String path;
    String format;
    List<TreeNode> children;


    @Override
    public List<TreeNode> children() {
        return this.children;
    }

    @Override
    public String toString() {
        return "FileScan{" +
                "nodeName='" + nodeName + '\'' +
                ", path='" + path + '\'' +
                ", format='" + format + '\'' +
                '}';
    }
}
