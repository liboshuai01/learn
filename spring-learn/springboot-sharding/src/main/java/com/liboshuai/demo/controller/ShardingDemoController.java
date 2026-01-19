package com.liboshuai.demo.controller;

import com.liboshuai.demo.entity.UserEntity;
import com.liboshuai.demo.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.util.Optional;

@Slf4j
@RestController
@RequestMapping("/demo")
@RequiredArgsConstructor
public class ShardingDemoController {

    private final UserRepository userRepository;

    /**
     * 演示写操作：创建用户
     * 该请求会由 ShardingSphere 路由到写数据源 (master1)
     * @param user 用户实体
     * @return 创建后的用户实体
     */
    @PostMapping("/users")
    public ResponseEntity<UserEntity> createUser(@RequestBody UserEntity user) {
        log.debug("Creating a new user: {}", user.getName());
        // JpaRepository.save() 是一个写操作
        UserEntity savedUser = userRepository.save(user);
        log.debug("User created with ID: {}", savedUser.getId());
        return ResponseEntity.ok(savedUser);
    }

    /**
     * 演示读操作：根据ID查询用户
     * 该请求会由 ShardingSphere 路由到读数据源 (slave1)
     * @param id 用户ID
     * @return 查询到的用户
     */
    @GetMapping("/users/{id}")
    public ResponseEntity<UserEntity> getUserById(@PathVariable Long id) {
        log.debug("Querying user by ID: {}", id);
        // JpaRepository.findById() 是一个读操作
        Optional<UserEntity> user = userRepository.findById(id);
        return user.map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * 演示特殊情况：事务内的读操作
     * 虽然是读操作，但由于被 @Transactional 注解包裹，ShardingSphere 为保证数据一致性，
     * 会将此请求强制路由到写数据源 (master1)。
     * @param id 用户ID
     * @return 查询到的用户
     */
    @GetMapping("/users/transactional-read/{id}")
    @Transactional(readOnly = true) // 即使是只读事务，也会路由到主库
    public ResponseEntity<UserEntity> getUserByIdInTransaction(@PathVariable Long id) {
        log.debug("Querying user by ID within a transaction: {}", id);
        Optional<UserEntity> user = userRepository.findById(id);
        log.debug("Data consistency is ensured by reading from the primary data source.");
        return user.map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}