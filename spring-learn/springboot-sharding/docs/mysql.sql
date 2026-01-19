DROP TABLE IF EXISTS `user`;
CREATE TABLE `user`
(
    `id`     bigint UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键ID',
    `name`   varchar(256)    DEFAULT NULL COMMENT '名字',
    `age`    bigint UNSIGNED DEFAULT NULL COMMENT '年龄',
    `gender` TINYINT(1)      DEFAULT NULL COMMENT '性别：0-false,1-true',
    PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB
  CHARACTER SET = utf8mb4
  COLLATE = utf8mb4_bin COMMENT = '用户表';