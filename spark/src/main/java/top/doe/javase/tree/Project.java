package top.doe.javase.tree;

import lombok.*;

import java.util.List;


@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Project implements TreeNode{

    String nodeName = "project";
    List<String> fields;
    List<TreeNode> children;

    @Override
    public List<TreeNode> children() {
        return this.children;
    }

    @Override
    public String toString() {
        return "Project{" +
                "nodeName='" + nodeName + '\'' +
                ", fields=" + fields +
                '}';
    }
}
