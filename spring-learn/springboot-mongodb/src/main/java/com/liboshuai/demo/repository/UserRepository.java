package com.liboshuai.demo.repository;

import com.liboshuai.demo.entity.UserEntity;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * 用户数据访问仓库
 * MongoRepository<UserDao, String> 提供了完整的CRUD基础功能。
 * - UserDao: 实体类
 * - String: 主键ID的类型
 */
@Repository
public interface UserRepository extends MongoRepository<UserEntity, String> {

    /**
     * 自定义查询：根据用户名查找用户列表（因为用户名可能不唯一）。
     * Spring Data会根据方法名自动生成 "find by name" 的查询。
     *
     * @param name 用户名
     * @return 匹配的用户列表
     */
    List<UserEntity> findByName(String name);

}