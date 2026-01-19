package com.liboshuai.demo.controller;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.liboshuai.demo.entity.UserEntity;
import com.liboshuai.demo.mapper.UserMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/demo")
@RequiredArgsConstructor
public class MPDynamicDemoController {

    private final UserMapper userMapper;

    /**
     * 查询master库
     */
    @PostMapping("/findMaster")
    public ResponseEntity<List<UserEntity>> findMaster() {
        List<UserEntity> userEntityList = userMapper.selectList(null);
        return ResponseEntity.ok(userEntityList);
    }

    /**
     * 查询slave1库
     */
    @DS("slave_1")
    @PostMapping("/findSlave1")
    public ResponseEntity<List<UserEntity>> findSlave1() {
        List<UserEntity> userEntityList = userMapper.selectList(null);
        return ResponseEntity.ok(userEntityList);
    }

    /**
     * 查询slave2库
     */
    @DS("slave_2")
    @PostMapping("/findSlave2")
    public ResponseEntity<List<UserEntity>> findSlave2() {
        List<UserEntity> userEntityList = userMapper.selectList(null);
        return ResponseEntity.ok(userEntityList);
    }

}