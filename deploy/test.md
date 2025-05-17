# 集群测试
## 测试一 部署 
```bash
    go run deploy/main.go deploy
    go run deploy/main.go start
```
## 测试二 集群状态
```bash
    go run deploy/main.go get-nodes
```

## 测试三 get set delete
```bash
    go run deploy/main.go set --key mykey --value myvalue --tikvaddr 127.0.0.1:20160
    go run deploy/main.go get --key mykey --tikv_addr 127.0.0.1:20160
    go run deploy/main.go delete --key mykey --tikv_addr 127.0.0.1:20160
```
    
## 测试四 事务
```bash
    go run deploy/main.go setByTxn --key mykey --value myvalue --tikv_addr 127.0.0.1:20160 --startVersion 1 --commitVersion 2
    go run deploy/main.go getByTxn --key mykey --tikv_addr 127.0.0.1:20160 --commitVersion 2
```

# TiDB 测试
## 部署
```bash
    go run deploy/main.go deploy -n 1
    go run deploy/main.go start -n 1
```

## 测试
```bash
    ./tidb/bin/tidb-server --store=tikv --path="127.0.0.1:2379"
    mysql -u root -h 127.0.0.1 -P 4000
```

```sql
    SHOW DATABASES;
    CREATE DATABASE mytest;
    USE mytest;
    CREATE TABLE USERS (id INT PRIMARY KEY, name VARCHAR(255));
    INSERT INTO USERS (id, name) VALUES (1, 'Alice'), (2, 'Bob');
    SELECT * FROM USERS;
```
事务测试
```sql
    BEGIN;
    INSERT INTO USERS (id, name) VALUES (3, 'Charlie');
    SELECT * FROM USERS;
    ROLLBACK;
    SELECT * FROM USERS;
```
