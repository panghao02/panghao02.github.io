# 使用 Spark DataFrame 和 SQL 进行数据处理与查询

本文将介绍如何使用 Spark DataFrame 和 SQL 进行数据处理与查询。我们将会通过两个场景展示如何创建 DataFrame、注册临时表以及进行数据查询。

## 场景一：创建临时表并进行查询

在这个场景中，我们将会创建一个包含人名和年龄信息的 DataFrame，并将其注册为一个临时表，然后进行一些简单的 SQL 查询。

### 创建 DataFrame 并注册临时表

首先，我们创建一个包含人名和年龄信息的 DataFrame：

```
val humanDF = List((1,"sun",18),(2,"li",23),(3, "zhao", 15)).toDF("id","name","age")
humanDF.registerTempTable("t_human")
```

### 查询年龄最大的前两名人的信息

```
spark.sql("select * from t_human order by age desc limit 2").show()
```

查询结果显示年龄最大的前两名人的信息。

### 查询年龄大于25的人的信息

```
spark.sql("select * from t_human where age > 25").show()
```

查询结果显示年龄大于25岁的人的信息。

## 场景二：从文件中读取数据并进行查询

在这个场景中，我们将会从一个文本文件中读取数据，创建 DataFrame，注册临时表，然后进行一些查询操作。

### 从文件中读取数据并创建 DataFrame

首先，从文件中读取数据并创建 RDD：

```
val lineRDD = sc.textFile("/text/person.txt").map(_.split(" "))
```

定义 `Person` 类：

```
case class Person(id: Int, name: String, age: Int)
```

转换数据：

```
scala > val personRDD = lineRDD.map(x => Person(x(0).toInt, x(1), x(2).toInt))
```

转换为 DataFrame：

```
val personDF = personRDD.toDF()
```

注册为临时表：

```
personDF.createOrReplaceTempView("t_person")
```

显示 DataFrame：

```
personDF.show()
```

### 一些常见的 DataFrame 操作

```
personDF.tail(2).foreach(println)
personDF.first()
personDF.take(1)
personDF.select(personDF.col("name")).show()
personDF.select(personDF.col("name"), personDF.col("age")).show()
```

注意以下两种方式是错误的：

```
personDF.select(x => x.col("name")).show() // 错误
personDF.select(_.col("name")).show()      // 错误
```

正确的方式：

```
personDF.select($"name").show()
```

### 查询年龄最大的前两名人的信息

```
spark.sql("select * from t_person order by age desc limit 2").show()
```

查询结果显示年龄最大的前两名人的信息。

### 查询年龄大于25的人的信息

```
spark.sql("select * from t_person where age > 25").show()
```

查询结果显示年龄大于25岁的人。

## 数据源

假设数据文件 `/text/person.txt` 的内容如下：

```
id name age
1 sun 18
2 li 20
3 zhao 15
```

通过上述步骤，我们展示了如何使用 Spark DataFrame 和 SQL 进行数据处理与查询，包括从文件中读取数据、创建 DataFrame、注册临时表以及进行各种查询操作。这些操作在实际的数据处理和分析中非常常见且有用。