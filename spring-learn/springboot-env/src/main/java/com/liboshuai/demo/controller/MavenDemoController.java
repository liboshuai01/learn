package com.liboshuai.demo.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/demo")
public class MavenDemoController {

    @Value("${demo.value}")
    private String value;

    @RequestMapping("/getValue")
    public String getValue(){
        return "不同环境获取到的value值: " + value;
    }
}
