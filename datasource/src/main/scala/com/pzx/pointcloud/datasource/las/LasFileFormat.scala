package com.pzx.pointcloud.datasource.las

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import com.pzx.pointcloud.datasource.las.PointFieldName.{X, Y, Z}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, UnsafeProjection}


class LasFileFormat extends FileFormat with DataSourceRegister with Serializable{

  override def shortName(): String = "las"

  override def toString: String = "las"

  override def equals(obj: Any): Boolean = obj.isInstanceOf[LasFileFormat]

  override def hashCode(): Int = getClass.hashCode()

  override def isSplitable(sparkSession: SparkSession, options: Map[String, String], path: Path): Boolean = true

  //由于不明白所输入的LAS文件的版本以及是否输入的多个文件属于不同的版本，所以这里只返回XYZ列
  override def inferSchema(sparkSession: SparkSession,
                           options: Map[String, String],
                           files: Seq[FileStatus]): Option[StructType] = {

    Some(StructType(
      Seq(StructField(X, DoubleType, false), StructField(Y, DoubleType, false), StructField(Z, DoubleType, false))
    ))
  }


  override protected def buildReader(sparkSession: SparkSession,
                                     dataSchema: StructType,//可以不用管，返回requiredSchema定义的列即可
                                     partitionSchema: StructType,//可以不用管，buildReaderWithPartitionValues会调用builderReader方法，再将partitionColumnValue加上
                                     requiredSchema: StructType,//可以不用必须返回requiredSchema，返回dataSchema也可，FileSourceScanExec上的ProjectExec会再一次裁剪
                                     filters: Seq[Filter],//可以不用管，FileSourceScanExec上，还有FilterExec会再一次过滤
                                     options: Map[String, String],
                                     hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {

    //Configuration默认是不能序列化的
    val broadcastConf = sparkSession.sparkContext.broadcast(configurationToString(hadoopConf))

    file: PartitionedFile => {
      val conf = stringToConfiguration(broadcastConf.value)

      val lasFilePointReader = new LasFilePointReader(file, conf)
      //TODO  利用requiredSchema对数据进行裁剪，可将这步推到lasFilePointReader中去做
      val unsafeProjection =  schemaProjection(requiredSchema, lasFilePointReader.pointSchema)
      lasFilePointReader.map(unsafeProjection.apply)
    }
  }

  private def configurationToString(conf : Configuration) : String = {
    val byteArrayOutputStream = new ByteArrayOutputStream()
    conf.write(new DataOutputStream(byteArrayOutputStream))
    byteArrayOutputStream.toString("utf-8")
  }

  private def stringToConfiguration(s : String) : Configuration = {
    val conf = new Configuration(false)
    conf.readFields(new DataInputStream(new ByteArrayInputStream(s.getBytes("utf-8"))))
    conf
  }

  //根据输入和所需的schema，创建对应的UnsafeProjection
  private def schemaProjection(requiredSchema : StructType, inputSchema : StructType) : UnsafeProjection = {
    //对于点数据的所有列信息，裁剪掉不需要的列，只留下requiredSchema包含的列，模仿ProjectExec的写法
    val inputAttributes = inputSchema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
    //必须使用上面已创建的Attribute，因为UnsafeProjection.create会使用Expression的exprid（每个对象唯一）进行绑定，从而project对应的列
    val requiredAttributes = requiredSchema.map(f => inputAttributes(inputSchema.fieldIndex(f.name)))
    UnsafeProjection.create(requiredAttributes, inputAttributes)
  }



  override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = {
    throw new RuntimeException("Las writer is not provided at present !")
  }

}
