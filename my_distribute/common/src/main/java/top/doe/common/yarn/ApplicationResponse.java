package top.doe.common.yarn;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class ApplicationResponse implements Serializable {

    private String status;  // success |  fail

    private List<String> containerIds;

    private String jobId;

}
