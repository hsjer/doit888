package top.doe.javase.tree;

import lombok.*;

import java.util.List;


@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Filter implements TreeNode {

    String nodeName = "filter";
    String exp;
    List<TreeNode> children;

    @Override
    public List<TreeNode> children() {
        return this.children;
    }

    @Override
    public String toString() {
        return "Filter{" +
                "nodeName='" + nodeName + '\'' +
                ", exp='" + exp + '\'' +
                '}';
    }
}
