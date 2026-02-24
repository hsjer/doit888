package top.doe.common.yarn;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AppLaunchContext implements Serializable {

    private byte[] jarBytes;
    private String mainClass;
    private String paramJson;

    private String applicationId;
    private String taskId;

}
