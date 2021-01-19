package com.pzx.pointcloud.datasource.las.strategy

import com.pzx.pointcloud.datasource.las.{LasFileHeader, LasFileReader}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, Literal, NamedExpression, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count, Max, Min}
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.mutable.ListBuffer

/**
  * 检测表达式改进思路:可以利用Expression的 tranform方法遍历从上到下检测有无Max、Min等相关表达式
  * 计算改进思路:可以利用Expression的 tranform方法遍历从上到下，将Max Min等表达式转换为字面量表达式，最后再计算表达式，获取最终值
  *
  * @param aggregateExpressions
  * @param filePaths
  */
case class LasAggregationExec(aggregateExpressions: Seq[NamedExpression],
                              filePaths : Array[String]) extends SparkPlan {

  //读取文件，获取文件头信息
  lazy val lasFileHeaders : Array[LasFileHeader] = filePaths.flatMap(filePath => {
    val hadoopPath = new Path(filePath)
    val hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConf()
    val fs = hadoopPath.getFileSystem(hadoopConf)
    val iterator = fs.listFiles(hadoopPath, true)
    var headers : List[LasFileHeader] = Nil
    while (iterator.hasNext){
      val lasFileReader = new LasFileReader(iterator.next().getPath, hadoopConf)
      headers = lasFileReader.getLasFileHead :: headers
      lasFileReader.close()
    }
    headers
  })


  override def output: Seq[Attribute] =  aggregateExpressions.map(a => a.toAttribute)

  override def children: Seq[SparkPlan] = Nil

  //将AggregateExpression替换为Literal
  def replaceAggregateExpression(experssion: Expression) = experssion transform {
    case AggregateExpression(Min(AttributeReference("x", _, _, _)),_,_,_) => Literal(lasFileHeaders.map(_.getMinX).min)
    case AggregateExpression(Max(AttributeReference("x", _, _, _)),_,_,_) => Literal(lasFileHeaders.map(_.getMaxX).min)
    case AggregateExpression(Min(AttributeReference("y", _, _, _)),_,_,_) => Literal(lasFileHeaders.map(_.getMinY).min)
    case AggregateExpression(Max(AttributeReference("y", _, _, _)),_,_,_) => Literal(lasFileHeaders.map(_.getMaxY).min)
    case AggregateExpression(Min(AttributeReference("z", _, _, _)),_,_,_) => Literal(lasFileHeaders.map(_.getMinZ).min)
    case AggregateExpression(Max(AttributeReference("z", _, _, _)),_,_,_) => Literal(lasFileHeaders.map(_.getMaxZ).min)
    case AggregateExpression(Count(_),_,_,_) => Literal(lasFileHeaders.map(_.getNumberOfPointRecords).sum)
  }

  override protected def doExecute(): RDD[InternalRow] = {

    println("LAS AggregatePlan optimization !")

    var rowValues = ListBuffer[Any]()
    var i = 0
    while (i < aggregateExpressions.length){
      val expression = replaceAggregateExpression(aggregateExpressions(i)).asInstanceOf[NamedExpression]
      rowValues += expression.eval(InternalRow.empty)
      i += 1
    }
    sqlContext.sparkContext.parallelize(
      Seq(UnsafeProjection.create(output,output).apply(InternalRow.fromSeq(rowValues))),
      1)
  }
}
