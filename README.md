## 引言

Hive-JDBC-Proxy是一个高性能的HiveServer2和Spark ThriftServer的代理服务，具备负载均衡、基于规则转发Hive JDBC的请求给到HiveServer2和Spark ThriftServer的能力。

Hive-JDBC-Proxy甚至可以根据SQL的复杂度和执行性能，实现为用户智能选择是提交给Spark ThriftServer还是HiveServer2。

如下图：

![架构图](docs/zh_CN/images/readme/架构图.png)

----

## 核心特点

- 支持分组，组内可以配置多个目标HiveServer2或Spark ThriftServer，支持基于规则和负载均衡的方式代理到其中的任意目标服务。

- 支持复杂的规则限制（**以下规则都支持用户自定义处理规则**），如：

  1. 同一个IP下，只允许一个Session。
  
  2. 同一个用户，只允许一个Session。
  
  3. 复杂的登录校验。
  
  4. 支持复杂的处理规则，如检查用户SQL是否合法、修改用户提交的SQL等。
  
  5. 支持statistics统计，统计用户的SQL执行时长和执行频率。
  
  6. 支持自定义负载均衡的规则。
  
- 高并发的处理能力。

- 支持动态加载分组。

----

## Running in your IDE

推荐使用IntelliJ IDEA.

1. clone到本地，打开IntelliJ IDEA加载Hive-JDBC-Proxy

2. 修改src/main/resources

```
    # login.properties的修改
    # 允许指定多个，username和password指Hive JDBC Client端到Proxy的用户密码认证
    username=password

    # thrift.xml
    # 修改thrift的server，指定实际代理给的目标端HiveServer2服务或Spark ThriftServer服务
    # 允许指定多个server，当存在多个server时，会自动负载均衡
    <server name="" username="hadoop" password="123" valid="true" host="目标HiveServer2的IP" port="目标HiveServer2的Port" maxThread="5" loginTimeout="2h" />
    
```

3. 启动Hive-JDBC-Proxy

  MainClass： com.enjoyyin.hive.proxy.jdbc.server.JDBCProxyServer
  
4. 启动测试类

  test模块提供了测试类，可以直接访问Proxy服务。
  
  MainClass： com.enjoyyin.hive.proxy.jdbc.test.ThriftServerTest

## QuickUse

1. clone Hive-JDBC-Proxy

```bash
 git clone https://github.com/wushengyeyouya/Hive-JDBC-Proxy.git
```

2. 打包

```bash
    cd Hive-JDBC-Proxy/assembly
    mvn clean package
    
```

3. 解压部署

```bash
    tar -zxvf hive-jdbc-proxy-assembly-1.0.0-dist.tar.gz
```

----

## RoadMap

1. 支持基于规则，智能选择提交给Spark ThriftServer还是HiveServer2。

2. 对接Apache Calcite，能自动识别Bad SQL，实现基于行和列的权限控制

----

## Contributing

欢迎参与贡献。

----

## Communication

欢迎给Hive-JDBC-Proxy提issue，我将尽快响应。

如您有任何建议，也欢迎给Hive-JDBC-Proxy提issue。