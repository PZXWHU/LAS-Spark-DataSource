package com.pzx.pointcloud.datasource.las.strategy

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.datasources._
import com.pzx.pointcloud.datasource.las.{LasFileFormat, LasFileReader}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{Count, Max, Min}
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import com.pzx.pointcloud.datasource.las.LasFileHeader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, GenerateUnsafeProjection, UnsafeRowWriter}

import scala.reflect.ClassTag
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{ByteType, DataType, DecimalType, DoubleType, IntegerType, LongType, ShortType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.expressions.aggregate._

import scala.collection.mutable.ListBuffer


object LasAggregationStrategy extends SparkStrategy{

  //检查aggregateExpressions是否符合条件
  private def checkAggregateExpressions(aggregateExpressions : Seq[Expression]): Boolean = {
    for(expression <- aggregateExpressions){
      val success =  new Array[Boolean](1)
      expression.foreach{
        case Min(AttributeReference("x", _, _, _)) => success(0) = true
        case Max(AttributeReference("x", _, _, _)) => success(0) = true
        case Min(AttributeReference("y", _, _, _)) => success(0) = true
        case Max(AttributeReference("y", _, _, _)) => success(0) = true
        case Min(AttributeReference("z", _, _, _)) => success(0) = true
        case Max(AttributeReference("z", _, _, _)) => success(0) = true
        case Count(_) => success(0) = true
        case _ =>
      }
      if (!success(0)) return false
    }
    true
  }

  //检查LogicalPlan是否是LogicalRelation或者Project， LogicalRelation中的baseRelation是否是HadoopFsRelation且包含LasFileFormat
  private def extractHadoopFsRelation(logicalPlan: LogicalPlan): HadoopFsRelation ={
    //获取LogicalRelation
    val logicalRelation = logicalPlan match {
      case Project(_, logicalRelation : LogicalRelation) => logicalRelation
      case logicalRelation : LogicalRelation => logicalRelation
      case _ => null
    }
    //判断LogicalRelation中的baseRelation是否是HadoopFsRelation，且其中的fileFormat是否是LasFileFormat
    logicalRelation.relation match {
      case hadoopFsRelation: HadoopFsRelation =>
        if (hadoopFsRelation.fileFormat.isInstanceOf[LasFileFormat]) hadoopFsRelation else null
      case _ => null
    }
  }

  override def apply(plan: LogicalPlan): Seq[SparkPlan] =  plan match {

    case Aggregate(Nil, aggregateExpressions, logicalPlan)  =>{
      val hadoopFsRelation = extractHadoopFsRelation(logicalPlan)
      if (hadoopFsRelation != null && checkAggregateExpressions(aggregateExpressions)){
        LasAggregationExec(aggregateExpressions, hadoopFsRelation.inputFiles) :: Nil
      }else
        Nil
    }
    case _ => Nil
  }

  def registerStrategy(sparkSession: SparkSession) = {
    val strategies = sparkSession.experimental.extraStrategies.diff(Seq(LasAggregationStrategy))
    sparkSession.experimental.extraStrategies = LasAggregationStrategy +: strategies
  }

}
