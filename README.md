# LAS Spark DataSoure

本项目扩展SparkSQL DataSource API，提供了读取LAS文件的SparkSQL接口。

- 实现了Las文件的读取功能，兼容1.0-1.4版本。
- 扩展了Spark Strategy，利用Las文件头实现MAX、MIN、COUNT聚合操作，而不用遍历全部点数据

## Spark版本

本项目基于Spark-2.4.4开发



## LAS文件

LAS文件格式是一种用于交换和储存激光雷达点云数据的一种文件格式，是美国摄影测量与遥感学会（ASPRS）指定的一种开放的二进制格式，该格式被广泛使用，并被视为激光雷达数据的行业标准。目前LAS文件共有五种版本格式，即1.0，1.1，1.2，1.3，1.4。具体文件格式可在<https://www.asprs.org/divisions-committees/lidar-division/laser-las-file-format-exchange-activities>处下载。



## 使用



```java
SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();

LasAggregationStrategy.registerStrategy(sparkSession);//注册自定义策略

//schema可根据自己的需要指定，但是不能指定输入LAS文件版本中不存在的点属性
List<StructField> fields = new ArrayList<>();
fields.add(PointStructField.X_Field());
fields.add(PointStructField.Y_Field());
fields.add(PointStructField.Z_Field());
fields.add(PointStructField.INTENSITY_Field());
fields.add(PointStructField.CLASSFICATION_Field());
StructType scheme = DataTypes.createStructType(fields);


Dataset<Row> dataset = sparkSession.read()
    //.format("com.pzx.pointcloud.datasource.las.LasFileFormat") //可以只用las简写
    .format("las")
    .schema(scheme) //如果不使用自定义scheme，则只会返回XYZ列，这是为了兼容输入LAS文件具有不同的版本
    .load("D:\\wokspace\\点云数据集\\大数据集与工具\\data\\las\\\elkrnefst.las");

dataset.show();

/*
+---------+----------+------------------+---------+--------------+
|        x|         y|                 z|intensity|classification|
+---------+----------+------------------+---------+--------------+
|557635.82|5130797.52|           1255.09|        0|             9|
|557634.36|5130797.63|           1255.29|        0|             9|
|557632.85|5130797.74|           1255.29|        0|             9|
|557631.36|5130797.85|            1255.3|        0|             9|
|557629.84|5130797.95|           1255.23|        0|             9|
|557628.31|5130798.05|           1255.03|        0|             9|
|557612.46|5130797.93|           1250.83|        0|             9|
|557614.32|5130797.89|           1252.78|        0|             9|
|557615.72|5130797.78|1252.6100000000001|        0|             9|
|557617.03|5130797.64|           1251.96|        0|             9|
+---------+----------+------------------+---------+--------------+
*/

Dataset<Row> dataset1 = dataset.select(min("x"),min("y"),min("z"),
                max("x"),max("y"),max("z"), count("x"));

dataset1.explain();
/*
== Physical Plan ==
LasAggregation [min(x#0) AS min(x)#36, min(y#1) AS min(y)#37, min(z#2) AS min(z)#38, max(x#0) AS max(x)#39, max(y#1) AS max(y)#40, max(z#2) AS max(z)#41, count(x#0) AS count(x)#42L], [file:///D:/wokspace/点云数据集/大数据集与工具/data/las/elkrnefst.las]
*/

dataset1.show();
/*
+---------+----------+------+---------+----------+-------+--------+
|   min(x)|    min(y)|min(z)|   max(x)|    max(y)| max(z)|count(x)|
+---------+----------+------+---------+----------+-------+--------+
|553512.22|5129041.21|815.14|557652.58|5130798.05|1310.44| 1989669|
+---------+----------+------+---------+----------+-------+--------+
*/
```



## 参考

<https://github.com/IGNF/spark-iqmulus>（此项目只支持Spark 1.X，由于Spark2.X和Spark1.X在SparkSQL源码方面具有较大改动，所以此项目只能在Spark1.X平台上运行）



## 改进

LAS write API

