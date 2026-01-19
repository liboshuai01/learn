package cn.liboshuai.learn.springboot.mongodb.controller;

import cn.liboshuai.learn.springboot.mongodb.entity.UserEntity;
import cn.liboshuai.learn.springboot.mongodb.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/demo/users")
@RequiredArgsConstructor
public class UserController {

    private final UserRepository userRepository;

    /**
     * CREATE - 创建一个新用户
     * HTTP POST /demo/users
     *
     * @param user 用户数据，从请求体中获取
     * @return 创建成功后的用户对象
     */
    @PostMapping
    public UserEntity createUser(@RequestBody UserEntity user) {
        // save() 方法会处理插入操作。如果对象包含id，则会尝试更新。
        // 为确保是创建，可以先将id设为null。
        user.setId(null);
        return userRepository.save(user);
    }

    /**
     * READ - 获取所有用户
     * HTTP GET /demo/users
     *
     * @return 用户列表
     */
    @GetMapping
    public List<UserEntity> getAllUsers() {
        return userRepository.findAll();
    }

    /**
     * READ - 根据ID获取单个用户
     * HTTP GET /demo/users/{id}
     *
     * @param id 用户ID，从URL路径中获取
     * @return 如果找到用户，返回用户信息和200 OK；否则返回404 Not Found。
     */
    @GetMapping("/{id}")
    public ResponseEntity<UserEntity> getUserById(@PathVariable String id) {
        Optional<UserEntity> userOptional = userRepository.findById(id);
        // 使用ResponseEntity来处理"未找到"的情况，这是REST API的最佳实践
        return userOptional.map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * READ - 根据用户名自定义查询
     * HTTP GET /demo/users/by-name?name=some_name
     *
     * @param name 用户名，从URL查询参数中获取
     * @return 匹配的用户列表
     */
    @GetMapping("/by-name")
    public List<UserEntity> getUsersByName(@RequestParam String name) {
        return userRepository.findByName(name);
    }

    /**
     * UPDATE - 更新一个已存在的用户
     * HTTP PUT /demo/users/{id}
     *
     * @param id          要更新的用户ID
     * @param userDetails 包含更新信息的用户数据
     * @return 如果成功，返回更新后的用户和200 OK；如果用户不存在，返回404 Not Found。
     */
    @PutMapping("/{id}")
    public ResponseEntity<UserEntity> updateUser(@PathVariable String id, @RequestBody UserEntity userDetails) {
        return userRepository.findById(id)
                .map(user -> {
                    user.setName(userDetails.getName());
                    user.setAge(userDetails.getAge());
                    user.setGender(userDetails.getGender());
                    UserEntity updatedUser = userRepository.save(user); // save() 方法会识别ID并执行更新
                    return ResponseEntity.ok(updatedUser);
                })
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * DELETE - 根据ID删除一个用户
     * HTTP DELETE /demo/users/{id}
     *
     * @param id 要删除的用户ID
     * @return 如果成功，返回204 No Content；如果用户不存在，返回404 Not Found。
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteUser(@PathVariable String id) {
        // 先检查是否存在，以返回正确的状态码
        if (userRepository.existsById(id)) {
            userRepository.deleteById(id);
            return ResponseEntity.noContent().build(); // 204 No Content 是删除成功的标准响应
        } else {
            return ResponseEntity.notFound().build();
        }
    }
}