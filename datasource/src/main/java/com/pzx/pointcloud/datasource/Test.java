package com.pzx.pointcloud.datasource;

import com.pzx.pointcloud.datasource.las.strategy.LasAggregationExec;
import com.pzx.pointcloud.datasource.las.strategy.LasAggregationStrategy;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class Test {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();

        LasAggregationStrategy.registerStrategy(sparkSession);

        /*
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("x", DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField("y", DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField("z", DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField("intensity", DataTypes.ShortType, false));
        fields.add(DataTypes.createStructField("classification", DataTypes.ByteType, false));

         */

        //StructType scheme = DataTypes.createStructType(fields);

        Dataset<Row> dataset = sparkSession.read()
                //.format("com.pzx.pointcloud.datasource.las.LasFileFormat")
                .format("las")
                //.schema(scheme)
                .load("D:\\wokspace\\点云数据集\\大数据集与工具\\data\\las");


        //dataset.show(10);



        Dataset<Row> dataset1 = dataset.select(min("x"),min("y"),min("z"),
                max("x"),max("y"),max("z"), count("x"));

        dataset1.explain();//这一步和下一步的物理计划可能不同，因为下一步的action可能会影响物理计划

        System.out.println(dataset1.first().toString());
        //System.out.println(dataset1.first());//这个时候就不会出现expresion上带着奇怪的Cast

        dataset.show();







    }

}
