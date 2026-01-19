package com.liboshuai.demo.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

@Slf4j
@RestController
@RequestMapping("/demo")
public class RedisDemoController {

    @Resource
    private RedisTemplate<String,Object> redisTemplate;

    @RequestMapping("/setString")
    public String setString(String key, String value){
        redisTemplate.opsForValue().set(key,value);
        log.debug("设置成功...");
        return "设置成功";
    }

    @RequestMapping("/getString")
    public String getString(String key){
        String value = (String) redisTemplate.opsForValue().get(key);
        log.debug("查询成功...");
        return "返回值: " + value;
    }
}
