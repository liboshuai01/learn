package com.liboshuai.demo.dto;

import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KafkaEventDTO {
    private String name;
    private Integer age;
    private Boolean sex;
    private long timestamp; // 增加时间戳字段
}