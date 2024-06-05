该错误表明在尝试通过JDBC连接到Hive时，当前用户没有访问Hadoop文件系统（HDFS）中的`/tmp`目录的权限。具体来说，用户`anonymous`尝试执行操作时，被拒绝了权限。

### 解决方案

#### 1. 修改`/tmp`目录权限

为了使匿名用户能够访问`/tmp`目录，可以调整该目录的权限。以下命令可以在HDFS中修改目录权限：

```
hdfs dfs -chmod 1777 /tmp
```

这样设置会确保所有用户都有执行权限，并且目录的sticky bit设置为1（即使其他用户可以在该目录中创建文件，但只有文件的所有者或root可以删除或移动文件）。

#### 2. 使用正确的用户身份

确保您使用正确的用户身份来连接到Hive。如果您使用的是匿名用户，可以尝试使用具备适当权限的用户来连接。配置SparkSession时，可以指定用户身份：

```
import org.apache.spark.sql.SparkSession

val spark: SparkSession = SparkSession.builder()
    .appName("Spark SQL Application")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.warehouse.dir", "hdfs://node1:9000/user/hive/warehouse")
    .config("hive.metastore.uris", "thrift://node1:9083")
    .config("spark.hadoop.hadoop.security.authentication", "kerberos") // 如果使用Kerberos认证
    .config("spark.yarn.access.hadoopFileSystems", "hdfs://node1:9000")
    .enableHiveSupport()
    .getOrCreate()
```

如果使用Kerberos认证，确保您的环境已正确配置Kerberos。

#### 3. 检查HDFS ACLs（访问控制列表）

有时，即使目录权限设置正确，ACL（访问控制列表）也可能导致权限问题。您可以通过以下命令检查和修改ACL：

```
# 查看ACL
hdfs dfs -getfacl /tmp

# 修改ACL，确保其他用户可以访问
hdfs dfs -setfacl -m user:anonymous:rwx /tmp
```

#### 4. 确保Hadoop和Hive配置正确

确保您的Hadoop和Hive配置文件（如`core-site.xml`, `hdfs-site.xml`, `hive-site.xml`）中的用户和权限设置正确。配置文件中的设置需要与实际的用户和权限匹配。

### 总结

通过上述方法，可以解决权限问题，确保用户能够访问HDFS中的目录并连接到Hive。具体步骤包括修改目录权限、使用正确的用户身份、检查和修改ACL以及确保配置文件正确。