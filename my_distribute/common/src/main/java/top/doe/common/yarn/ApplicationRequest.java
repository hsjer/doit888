package top.doe.common.yarn;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class ApplicationRequest implements Serializable {
    private int containerNumb;
}
