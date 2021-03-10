package com.pzx.pointcloud.datasource.las

import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateUnsafeProjection, UnsafeRowWriter}
import org.apache.spark.sql.types._
import java.nio.{ByteBuffer, ByteOrder}

import PointFieldName._
import PointStructField._
import org.apache.spark.sql.catalyst.InternalRow

import scala.collection.mutable
import scala.util.control._

object PointFieldName{
  val X = "x"
  val Y = "y"
  val Z = "z"
  val INTENSITY = "intensity"
  val RNSE = "r_n_s_e"
  val CLASSFICATION = "classification"
  val SCAN_ANGLE_RANK_LEFT_SIDE = "scan_angle_rank_left_side"
  val USER_DATA = "user_data"
  val POINT_SOURCE_ID = "point_source_id"
  val GPS_TIME = "gps_time"
  val R = "r"
  val G = "g"
  val B = "b"
  val WAVE_PACKET_DESCRIPTOR_INDEX = "wave_packet_descriptor_index"
  val BYTE_OFFSET_TO_WAVEFORM_DATA = "byte_offset_to_waveform_data"
  val WAVEFORM_PACKET_SIZE_IN_BYTES= "waveform_packet_size_in_bytes"
  val RETURN_POINT_WAVEFORM_LOCATION = "return_point_waveform_location"
  val PARAMETRIC_DX = "parametric_dx"
  val PARAMETRIC_DY = "parametric_dy"
  val PARAMETRIC_DZ = "parametric_dz"
  val RNCSSE = "r_n_c_s_s_e"
  val SCAN_ANGLE = "scan_angle"
  val NIR = "nir"

}

object PointStructField{
  lazy val X_Field = StructField(X, DoubleType, false)
  lazy val Y_Field = StructField(Y, DoubleType, false)
  lazy val Z_Field = StructField(Z, DoubleType, false)
  lazy val INTENSITY_Field = StructField(INTENSITY, ShortType, false)
  lazy val RNSE_Field = StructField(RNSE, ByteType, false)
  lazy val CLASSFICATION_Field = StructField(CLASSFICATION, ByteType, false)
  lazy val SCAN_ANGLE_RANK_LEFT_SIDE_Field = StructField(SCAN_ANGLE_RANK_LEFT_SIDE, ByteType, false)
  lazy val USER_DATA_Field = StructField(USER_DATA, ByteType, false)
  lazy val POINT_SOURCE_ID_Field = StructField(POINT_SOURCE_ID, ShortType, false)
  lazy val GPS_TIME_Field = StructField(GPS_TIME, DoubleType, false)
  lazy val R_Field = StructField(R, ShortType, false)
  lazy val G_Field = StructField(G, ShortType, false)
  lazy val B_Field = StructField(B, ShortType, false)
  lazy val WAVE_PACKET_DESCRIPTOR_INDEX_Field = StructField(WAVE_PACKET_DESCRIPTOR_INDEX, ByteType, false)
  lazy val BYTE_OFFSET_TO_WAVEFORM_DATA_Field = StructField(BYTE_OFFSET_TO_WAVEFORM_DATA, LongType, false)
  lazy val WAVEFORM_PACKET_SIZE_IN_BYTES_Field= StructField(WAVEFORM_PACKET_SIZE_IN_BYTES, IntegerType, false)
  lazy val RETURN_POINT_WAVEFORM_LOCATION_Field = StructField(RETURN_POINT_WAVEFORM_LOCATION, IntegerType, false)
  lazy val PARAMETRIC_DX_Field = StructField(PARAMETRIC_DX, IntegerType, false)
  lazy val PARAMETRIC_DY_Field = StructField(PARAMETRIC_DY, IntegerType, false)
  lazy val PARAMETRIC_DZ_Field = StructField(PARAMETRIC_DZ, IntegerType, false)
  lazy val RNCSSE_Field = StructField(RNCSSE, ShortType, false)
  lazy val SCAN_ANGLE_Field = StructField(SCAN_ANGLE, ShortType, false)
  lazy val NIR_Field = StructField(NIR, ShortType, false)

}


object LasFilePointParser{

  def apply(lasFileHeader : LasFileHeader, requiredSchema: StructType): LasFilePointParser = {
    val parser = lasFileHeader.getPointDataFormatID match {
      case 0 => new LasFilePointParser0
      case 1 => new LasFilePointParser1
      case 2 => new LasFilePointParser2
      case 3 => new LasFilePointParser3
      case 4 => new LasFilePointParser4
      case 5 => new LasFilePointParser5
      case 6 => new LasFilePointParser6
      case 7 => new LasFilePointParser7
      case 8 => new LasFilePointParser8
      case 9 => new LasFilePointParser9
      case 10 => new LasFilePointParser10
    }
    parser.requiredSchema = requiredSchema
    parser.xScale = lasFileHeader.getxScale()
    parser.yScale = lasFileHeader.getyScale()
    parser.zScale = lasFileHeader.getzScale()
    parser.xOffset = lasFileHeader.getxOffset()
    parser.yOffset = lasFileHeader.getyOffset()
    parser.zOffset = lasFileHeader.getzOffset()
    parser
  }

}

abstract class LasFilePointParser extends Serializable{

  val formatVersion : Int

  val pointBytesLen : Int

  val fieldNum : Int

  def pointSchema : StructType

  var xScale = 1.0

  var yScale = 1.0

  var zScale = 1.0

  var xOffset = 0.0

  var yOffset = 0.0

  var zOffset = 0.0

  var requiredSchema: StructType = pointSchema

  lazy val requiredSchemaNameMap : mutable.Map[String, Int] = {
    val map = new mutable.HashMap[String, Int]
    var index = 0
    for(schema <- requiredSchema){
      map.put(schema.name, index)
      index += 1
    }
    map
  }

  lazy val row = new GenericInternalRow(requiredSchema.length)

  private def readByteBuffer(byteBuffer: ByteBuffer, dataType: DataType): Any = (dataType) match {
    case ByteType => byteBuffer.get()
    case ShortType => byteBuffer.getShort()
    case IntegerType => byteBuffer.getInt()
    case LongType => byteBuffer.getLong()
    case DoubleType =>  byteBuffer.getDouble()
  }

  def parse(bytes : Array[Byte]) : InternalRow = {

    val byteBuffer = ByteBuffer.wrap(bytes)
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN)//LAS文件中均是以LITTLE_ENDIAN方式存储

    var fieldNum = 0
    val loop = new Breaks
    loop.breakable{
      for(schema <- pointSchema){
        val fieldIndex = requiredSchemaNameMap.get(schema.name)
        if(fieldIndex.nonEmpty){
          val fieldValue = readByteBuffer(byteBuffer, schema.dataType)
          row(fieldIndex.get) = schema.name match {
            case X => Integer2UnsignedInteger(fieldValue.asInstanceOf[Int]) * xScale + xOffset
            case Y =>  Integer2UnsignedInteger(fieldValue.asInstanceOf[Int]) * yScale + yOffset
            case Z => Integer2UnsignedInteger(fieldValue.asInstanceOf[Int]) * zScale + zOffset
            case _ => fieldValue
          }
          fieldNum += 1
        }else
          byteBuffer.position(byteBuffer.position() + schema.dataType.defaultSize)

        if(fieldNum >= requiredSchema.length)
          loop.break()
      }
    }
    row
  }

  def Integer2UnsignedInteger(fieldValue : Int) : Long = {
    (fieldValue >>> 32 & 1L << 31 ) | ( fieldValue & Integer.MAX_VALUE.toLong )
  }

}

class LasFilePointParser0 extends LasFilePointParser{

  override val formatVersion: Int = 0

  override val pointBytesLen: Int = 20

  override val fieldNum: Int = 9

  override def pointSchema: StructType = StructType(Seq(
    StructField(X, IntegerType, false),
    StructField(Y, IntegerType, false),
    StructField(Z, IntegerType, false),
    INTENSITY_Field,
    RNSE_Field,
    CLASSFICATION_Field,
    SCAN_ANGLE_RANK_LEFT_SIDE_Field,
    USER_DATA_Field,
    POINT_SOURCE_ID_Field
  ))

}

class LasFilePointParser1 extends LasFilePointParser0{

  override val formatVersion: Int = 1

  override val pointBytesLen: Int = 28

  override val fieldNum: Int = 10

  override def pointSchema: StructType = super.pointSchema
    .add(GPS_TIME_Field)


}

class LasFilePointParser2 extends LasFilePointParser0{

  override val formatVersion: Int = 2

  override val pointBytesLen: Int = 26

  override val fieldNum: Int = 12

  override def pointSchema: StructType = super.pointSchema
    .add(R_Field)
    .add(G_Field)
    .add(B_Field)

}

class LasFilePointParser3 extends LasFilePointParser2{

  override val formatVersion: Int = 3

  override val pointBytesLen: Int = 34

  override val fieldNum: Int = 13

  override def pointSchema: StructType = super.pointSchema
    .add(R_Field)
    .add(G_Field)
    .add(B_Field)

}

class LasFilePointParser4 extends LasFilePointParser1{

  override val formatVersion: Int = 4

  override val pointBytesLen: Int = 57

  override val fieldNum: Int = 17

  override def pointSchema: StructType = super.pointSchema
    .add(WAVE_PACKET_DESCRIPTOR_INDEX_Field)
    .add(BYTE_OFFSET_TO_WAVEFORM_DATA_Field)
    .add(WAVEFORM_PACKET_SIZE_IN_BYTES_Field)
    .add(RETURN_POINT_WAVEFORM_LOCATION_Field)
    .add(PARAMETRIC_DX_Field)
    .add(PARAMETRIC_DY_Field)
    .add(PARAMETRIC_DZ_Field)

}

class LasFilePointParser5 extends LasFilePointParser3{

  override val formatVersion: Int = 5

  override val pointBytesLen: Int = 63

  override val fieldNum: Int = 20

  override def pointSchema: StructType = super.pointSchema
    .add(WAVE_PACKET_DESCRIPTOR_INDEX_Field)
    .add(BYTE_OFFSET_TO_WAVEFORM_DATA_Field)
    .add(WAVEFORM_PACKET_SIZE_IN_BYTES_Field)
    .add(RETURN_POINT_WAVEFORM_LOCATION_Field)
    .add(PARAMETRIC_DX_Field)
    .add(PARAMETRIC_DY_Field)
    .add(PARAMETRIC_DZ_Field)

}

class LasFilePointParser6 extends LasFilePointParser{

  override val formatVersion: Int = 6

  override val pointBytesLen: Int = 30

  override val fieldNum: Int = 10

  override def pointSchema: StructType = StructType(Seq(
    StructField(X, IntegerType, false),
    StructField(Y, IntegerType, false),
    StructField(Z, IntegerType, false),
    INTENSITY_Field,
    RNCSSE_Field,
    CLASSFICATION_Field,
    USER_DATA_Field,
    SCAN_ANGLE_Field,
    POINT_SOURCE_ID_Field,
    GPS_TIME_Field
  ))

}

class LasFilePointParser7 extends LasFilePointParser6{

  override val formatVersion: Int = 7

  override val pointBytesLen: Int = 36

  override val fieldNum: Int = 13

  override def pointSchema: StructType = super.pointSchema
    .add(R_Field)
    .add(G_Field)
    .add(B_Field)

}

class LasFilePointParser8 extends LasFilePointParser7{

  override val formatVersion: Int = 8

  override val pointBytesLen: Int = 38

  override val fieldNum: Int = 14

  override def pointSchema: StructType = super.pointSchema
    .add(NIR_Field)

}

class LasFilePointParser9 extends LasFilePointParser6{

  override val formatVersion: Int = 9

  override val pointBytesLen: Int = 59

  override val fieldNum: Int = 17

  override def pointSchema: StructType = super.pointSchema
    .add(WAVE_PACKET_DESCRIPTOR_INDEX_Field)
    .add(BYTE_OFFSET_TO_WAVEFORM_DATA_Field)
    .add(WAVEFORM_PACKET_SIZE_IN_BYTES_Field)
    .add(RETURN_POINT_WAVEFORM_LOCATION_Field)
    .add(PARAMETRIC_DX_Field)
    .add(PARAMETRIC_DY_Field)
    .add(PARAMETRIC_DZ_Field)

}

class LasFilePointParser10 extends LasFilePointParser9{

  override val formatVersion: Int = 10

  override val pointBytesLen: Int = 67

  override val fieldNum: Int = 21

  override def pointSchema: StructType = super.pointSchema
    .add(R_Field)
    .add(G_Field)
    .add(B_Field)
    .add(NIR_Field)

}

