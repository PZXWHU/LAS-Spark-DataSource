package com.pzx.pointcloud.datasource.las

import java.io.Closeable
import java.nio.ByteBuffer

import org.apache.spark.sql.types._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import PointFieldName._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.types.{DoubleType, IntegerType}

class LasFilePointReader(file: PartitionedFile, conf: Configuration, requiredSchema: StructType)
  extends Iterator[InternalRow] with Closeable with Serializable{

  lazy private val lasFileReader = new LasFileReader(new Path(file.filePath), conf)

  lazy private val lasFileHeader = lasFileReader.getLasFileHead

  lazy private val parser = LasFilePointParser(lasFileHeader, requiredSchema)

  //parse返回的point schema中的XYZ是Int类型，需要转换为double
  lazy private[las] val pointSchema : StructType = StructType(
    parser.pointSchema.map(s =>{
    if (s.name == PointFieldName.X || s.name == PointFieldName.Y || s.name == PointFieldName.Z)
      StructField(s.name, DoubleType, s.nullable) else s
    })
  )

  //保证只读取点数据
  private val start = Math.max(file.start, lasFileHeader.getOffsetToPointData)
  private val end = Math.min(file.start + file.length - 1, lasFileHeader.getOffsetToPointDataEnd)

  private val pointBuffer = new Array[Byte](lasFileHeader.getPointDataRecordLength)

  //下一次读取点数据的位移
  private var pos = {
    //当前分片的部分字节可能被上一个分片读取，所以需要根据点数据的长度和点数据的起始位移进行判断
    val bias = (start - lasFileHeader.getOffsetToPointData) % lasFileHeader.getPointDataRecordLength
    if (bias == 0) start else start + lasFileHeader.getPointDataRecordLength - bias
  }

  override def hasNext: Boolean = {
    pos <= end
  }

  override def next(): InternalRow = {
    lasFileReader.read(pointBuffer, pos)
    pos += lasFileHeader.getPointDataRecordLength
    parser.parse(pointBuffer)
  }

  /*点坐标需要根据scale和offset进行转换
  private def projectCoordinates(row: InternalRow) : InternalRow = {

    for(field <- Seq(X, Y, Z)){
      val fieldIndex = pointSchema.fieldIndex(field)
      var fieldValue = row.getInt(fieldIndex).toLong
      //首先Integer转换为unsignedInteger，即long
      fieldValue = (fieldValue >>> 32 & 1L << 31 ) | ( fieldValue & Integer.MAX_VALUE.toLong )
      val (scale, offset) = field match {
        case X => (lasFileHeader.getxScale(), lasFileHeader.getxOffset())
        case Y => (lasFileHeader.getyScale(), lasFileHeader.getyOffset())
        case Z => (lasFileHeader.getzScale(), lasFileHeader.getzOffset())
      }
      row.setDouble(fieldIndex, fieldValue * scale + offset)

    }
    row
  }
   */

  override def close(): Unit = lasFileReader.close()

  //override def toMap[T, U](implicit ev: UnsafeRow <:< (T, U)): Map[T, U] = super.toMap
}
