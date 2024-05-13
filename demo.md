## 准备数据
* 进入MySQL容器
  ```
    docker-compose exec mysql mysql -uroot -p123456
  ```
* 创建数据和表，并填充数据。 
  ```
 CREATE DATABASE db_1;
 USE db_1;
 CREATE TABLE user_1 (
   id INTEGER NOT NULL PRIMARY KEY,
   name VARCHAR(255) NOT NULL DEFAULT 'flink',
   address VARCHAR(1024),
   phone_number VARCHAR(512),
   email VARCHAR(255)
 );
 INSERT INTO user_1 VALUES (110,"user_110","Shanghai","123567891234","user_110@foo.com");

 CREATE TABLE user_2 (
   id INTEGER NOT NULL PRIMARY KEY,
   name VARCHAR(255) NOT NULL DEFAULT 'flink',
   address VARCHAR(1024),
   phone_number VARCHAR(512),
   email VARCHAR(255)
 );
INSERT INTO user_2 VALUES (120,"user_120","Shanghai","123567891234","user_120@foo.com");
  ```

```
CREATE DATABASE db_2;
USE db_2;
CREATE TABLE user_1 (
  id INTEGER NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL DEFAULT 'flink',
  address VARCHAR(1024),
  phone_number VARCHAR(512),
  email VARCHAR(255)
);
INSERT INTO user_1 VALUES (110,"user_110","Shanghai","123567891234", NULL);

CREATE TABLE user_2 (
  id INTEGER NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL DEFAULT 'flink',
  address VARCHAR(1024),
  phone_number VARCHAR(512),
  email VARCHAR(255)
);
INSERT INTO user_2 VALUES (220,"user_220","Shanghai","123567891234","user_220@foo.com");
```

## 在 Flink SQL CLI 中使用 Flink DDL 创建表
* 开启 checkpoint
Checkpoint 默认是不开启的，我们需要开启 Checkpoint 来让 Iceberg 可以提交事务。并且，mysql-cdc 在 binlog 读取阶段开始前，需要等待一个完整的 checkpoint 来避免 binlog 记录乱序的情况。
```
-- 每隔 3 秒做一次 checkpoint                 
Flink SQL> SET execution.checkpointing.interval = 3s;
```

* 创建 MySQL 分库分表 source 表
创建 source 表 user_source 来捕获MySQL中所有 user 表的数据，在表的配置项 database-name , table-name 使用正则表达式来匹配这些表。
```
-- Flink SQL
Flink SQL> CREATE TABLE user_source (
    database_name STRING METADATA VIRTUAL,
    table_name STRING METADATA VIRTUAL,
    `id` DECIMAL(20, 0) NOT NULL,
    name STRING,
    address STRING,
    phone_number STRING,
    email STRING,
    PRIMARY KEY (`id`) NOT ENFORCED
  ) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'db_[0-9]+',
    'table-name' = 'user_[0-9]+'
  );
```

* 创建 Iceberg sink 表
创建 sink 表 all_users_sink，用来将数据加载至 Iceberg 中。在这个 sink 表，考虑到不同的 MySQL 数据库表的 id 字段的值可能相同，我们定义了复合主键 (database_name, table_name, id)。
```
-- Flink SQL
Flink SQL> CREATE TABLE all_users_sink (
    database_name STRING,
    table_name    STRING,
    `id`          DECIMAL(20, 0) NOT NULL,
    name          STRING,
    address       STRING,
    phone_number  STRING,
    email         STRING,
    PRIMARY KEY (database_name, table_name, `id`) NOT ENFORCED
  ) WITH (
    'connector'='iceberg',
    'catalog-name'='iceberg_catalog',
    'catalog-type'='hadoop',  
    'warehouse'='file:///tmp/iceberg/warehouse',
    'format-version'='2'
  );
```

## 流式写入 Iceberg
* 将数据从 MySQL 写入 Iceberg 中：
```
-- Flink SQL
Flink SQL> INSERT INTO all_users_sink select * from user_source;
```

* 使用下面的 Flink SQL 语句查询表 all_users_sink 中的数据：
```
Flink SQL> SELECT * FROM all_users_sink;
```

* 在 db_1.user_1 表中插入新的一行
```
INSERT INTO db_1.user_1 VALUES (111,"user_111","Shanghai","123567891234","user_111@foo.com");
```
* 更新 db_1.user_2 表的数据
```
--- db_1
UPDATE db_1.user_2 SET address='Beijing' WHERE id=120;
```

* 在 db_2.user_2 表中删除一行
```
--- db_2
DELETE FROM db_2.user_2 WHERE id=220;

* 每执行一步，我们就可以在 Flink Client CLI 中使用 SELECT * FROM all_users_sink 查询表 all_users_sink 来看到数据的变化。
```
