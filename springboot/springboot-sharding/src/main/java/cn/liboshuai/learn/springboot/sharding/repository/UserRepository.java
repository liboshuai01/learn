package cn.liboshuai.learn.springboot.sharding.repository;

import cn.liboshuai.learn.springboot.sharding.entity.UserEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface UserRepository extends JpaRepository<UserEntity, Long> {
}
