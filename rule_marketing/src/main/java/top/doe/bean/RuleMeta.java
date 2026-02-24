package top.doe.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.roaringbitmap.longlong.Roaring64Bitmap;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RuleMeta {
    private int id;
    private String rule_name;
    private int rule_model_id;
    private String rule_model_code; // 规则所属模型的运算机java源代码
    private String rule_model_classname;
    private Roaring64Bitmap rule_crowd_bitmap;
    private String rule_param_json;
    private int rule_status;
    private long history_statistic_endtime;
    private String op;
    private byte[] rule_crowd_bitmap_bytes;
}
