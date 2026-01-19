package cn.liboshuai.learn.springboot.mybatisplus.controller;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import cn.liboshuai.learn.springboot.mybatisplus.entity.UserEntity;
import cn.liboshuai.learn.springboot.mybatisplus.mapper.UserMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/demo")
@RequiredArgsConstructor
public class UserController {

    private final UserMapper userMapper;

    // 1. 新增用户
    @PostMapping("/user")
    public ResponseEntity<UserEntity> createUser(@RequestBody UserEntity user) {
        int result = userMapper.insert(user);
        if (result > 0) {
            log.info("新增用户成功，ID: {}", user.getId());
            return ResponseEntity.ok(user);
        }
        log.error("新增用户失败");
        return ResponseEntity.badRequest().build();
    }

    // 2. 根据ID查询用户
    @GetMapping("/user/{id}")
    public ResponseEntity<UserEntity> getUserById(@PathVariable Long id) {
        UserEntity user = userMapper.selectById(id);
        if (user != null) {
            log.info("查询到用户: {}", user.getName());
            return ResponseEntity.ok(user);
        }
        log.warn("未找到用户ID: {}", id);
        return ResponseEntity.notFound().build();
    }

    // 3. 更新用户信息
    @PutMapping("/user")
    public ResponseEntity<UserEntity> updateUser(@RequestBody UserEntity user) {
        int result = userMapper.updateById(user);
        if (result > 0) {
            log.info("更新用户成功，ID: {}", user.getId());
            return ResponseEntity.ok(user);
        }
        log.error("更新用户失败，ID: {}", user.getId());
        return ResponseEntity.badRequest().build();
    }

    // 4. 根据ID删除用户
    @DeleteMapping("/user/{id}")
    public ResponseEntity<Void> deleteUser(@PathVariable Long id) {
        int result = userMapper.deleteById(id);
        if (result > 0) {
            log.info("删除用户成功，ID: {}", id);
            return ResponseEntity.ok().build();
        }
        log.warn("删除用户失败，未找到ID: {}", id);
        return ResponseEntity.notFound().build();
    }

    // 5. 查询所有用户
    @GetMapping("/users")
    public ResponseEntity<List<UserEntity>> getAllUsers() {
        List<UserEntity> users = userMapper.selectList(null);
        log.info("查询到 {} 个用户", users.size());
        return ResponseEntity.ok(users);
    }

    // 6. 条件查询示例：按姓名查询
    @GetMapping("/users/name/{name}")
    public ResponseEntity<List<UserEntity>> getUsersByName(@PathVariable String name) {
        LambdaQueryWrapper<UserEntity> query = Wrappers.lambdaQuery();
        query.eq(UserEntity::getName, name);

        List<UserEntity> users = userMapper.selectList(query);
        log.info("按姓名 {} 查询到 {} 个用户", name, users.size());
        return ResponseEntity.ok(users);
    }

    // 7. 分页查询示例
    @GetMapping("/users/page")
    public ResponseEntity<IPage<UserEntity>> getUsersByPage(
            @RequestParam(defaultValue = "1") int current,
            @RequestParam(defaultValue = "10") int size) {

        Page<UserEntity> page = new Page<>(current, size);
        IPage<UserEntity> result = userMapper.selectPage(page, null);

        log.info("分页查询: 第 {} 页/每页 {} 条，共 {} 条记录",
                current, size, result.getTotal());
        return ResponseEntity.ok(result);
    }
}