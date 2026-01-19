package cn.liboshuai.learn.pekko.ask.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ResponseData implements CborSerializable {
    private String message;
}
