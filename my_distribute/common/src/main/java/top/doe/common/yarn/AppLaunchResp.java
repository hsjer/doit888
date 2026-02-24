package top.doe.common.yarn;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.xml.ws.BindingType;
import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AppLaunchResp implements Serializable {
    private String status;
}
