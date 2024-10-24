# GoKeeper
## 项目介绍
该项目是使用 Go语言基于 Bitcask 论文实现的一个 KV 存储引擎。

**论文**: [Bitcask - A log-structured hash index](assets/bitcask-intro.pdf)

**中文版论文**: [Bitcask论文中文版](assets/bitcask-intro-zh.pdf)

## 功能描述
- 基本的KV存储功能，支持`Put`、`Get`、`Delete`操作
- 支持批量写入操作
- 支持数据迭代
- 提供HTTP接口


## 编译运行
### 依赖
确保安装了Go开发环境，可以使用以下命令安装项目依赖：
```Go
go mod tidy
```

## 示例代码
以下是如何使用GoKeeper进行基本的KV存储操作的示例代码：
```Go
// 1.初始化 DB 实例
options := GoKeeper.DefaultOptions
options.DirPath = filepath.Join(os.TempDir(), "goKeeper")
db, err := GoKeeper.Open(options)
defer func(db *GoKeeper.DB) {
    err := db.Close()
    if err != nil {
        log.Println(err)
        return
    }
}(db)
if err != nil {
    panic(err)
}
```

## HTTP接口
### 数据操作
- **PUT接口**: 插入或更新键值对
    - URL: `/api/v1/goKeeper/kv`
    - 方法: `PUT`
    - 请求体: `{"key": "value"}`
    - 成功响应: `{"code": 200, "msg": "put success"}`

- **GET接口**: 获取键对应的值
    - URL: `/api/v1/goKeeper/kv?key={key}`
    - 方法: `GET`
    - 成功响应: `{"code": 200, "data": "value", "msg": "get value success"}`

- **DELETE接口**: 删除指定的键值对
    - URL: `/api/v1/goKeeper/kv?key={key}`
    - 方法: `DELETE`
    - 成功响应: `{"code": 200, "msg": "delete success"}`

### 其他操作
- **列出所有键**
    - URL: `/api/v1/goKeeper/listKey`
    - 方法: `GET`
    - 成功响应: `{"code": 200, "data": ["key1", "key2"], "msg": "list key success"}`

- **统计信息**
    - URL: `/api/v1/goKeeper/stat`
    - 方法: `GET`
    - 成功响应: `{"code": 200, "data": {"keyNum": 10, "dataFileNum": 2, "reclaimableSize": 1024, "diskSize": 10240}, "msg": "get stat success"}`