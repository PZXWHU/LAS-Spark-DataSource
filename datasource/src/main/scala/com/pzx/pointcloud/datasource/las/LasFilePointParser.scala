package com.pzx.pointcloud.datasource.las

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateUnsafeProjection, UnsafeRowWriter}
import org.apache.spark.sql.types._
import java.nio.{ByteBuffer, ByteOrder}
import PointFieldName._

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


object LasFilePointParser{

  def apply(pointFormatVersion : Int): LasFilePointParser = {
    pointFormatVersion match {
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
  }
}

abstract class LasFilePointParser extends Serializable{

  val formatVersion : Int

  val pointBytesLen : Int

  val fieldNum : Int

  def pointSchema : StructType

  lazy val unsafeRowWriter : UnsafeRowWriter = new UnsafeRowWriter(fieldNum)

  //在unsafeRowWriter对应的位置写入对应类型的值
  private def writeRowField(i : Int, byteBuffer: ByteBuffer, dataType: DataType) = (dataType) match {
      case ByteType => unsafeRowWriter.write(i, byteBuffer.get())
      case ShortType => unsafeRowWriter.write(i, byteBuffer.getShort())
      case IntegerType => unsafeRowWriter.write(i, byteBuffer.getInt())
      case LongType => unsafeRowWriter.write(i, byteBuffer.getLong())
      case DoubleType => unsafeRowWriter.write(i, byteBuffer.getDouble())
  }


  def parse(bytes : Array[Byte]) : UnsafeRow = {

    val byteBuffer = ByteBuffer.wrap(bytes)
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN)//LAS文件中均是以LITTLE_ENDIAN方式存储
    unsafeRowWriter.reset()//重置unsafeRowWriter

    var i = 0
    while (i < pointSchema.length){
      writeRowField(i, byteBuffer, pointSchema(i).dataType)
      i += 1
    }
    unsafeRowWriter.getRow
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
    StructField(INTENSITY, ShortType, false),
    StructField(RNSE, ByteType, false),
    StructField(CLASSFICATION, ByteType, false),
    StructField(SCAN_ANGLE_RANK_LEFT_SIDE, ByteType, false),
    StructField(USER_DATA, ByteType, false),
    StructField(POINT_SOURCE_ID, ShortType, false),
  ))

}

class LasFilePointParser1 extends LasFilePointParser0{

  override val formatVersion: Int = 1

  override val pointBytesLen: Int = 28

  override val fieldNum: Int = 10

  override def pointSchema: StructType = super.pointSchema
    .add(StructField(GPS_TIME, DoubleType, false))


}

class LasFilePointParser2 extends LasFilePointParser0{

  override val formatVersion: Int = 2

  override val pointBytesLen: Int = 26

  override val fieldNum: Int = 12

  override def pointSchema: StructType = super.pointSchema
    .add(StructField(R, ShortType, false))
    .add(StructField(G, ShortType, false))
    .add(StructField(B, ShortType, false))

}

class LasFilePointParser3 extends LasFilePointParser2{

  override val formatVersion: Int = 3

  override val pointBytesLen: Int = 34

  override val fieldNum: Int = 13

  override def pointSchema: StructType = super.pointSchema
    .add(StructField(R, ShortType, false))
    .add(StructField(G, ShortType, false))
    .add(StructField(B, ShortType, false))

}

class LasFilePointParser4 extends LasFilePointParser1{

  override val formatVersion: Int = 4

  override val pointBytesLen: Int = 57

  override val fieldNum: Int = 17

  override def pointSchema: StructType = super.pointSchema
    .add(StructField(WAVE_PACKET_DESCRIPTOR_INDEX, ByteType, false))
    .add(StructField(BYTE_OFFSET_TO_WAVEFORM_DATA, LongType, false))
    .add(StructField(WAVEFORM_PACKET_SIZE_IN_BYTES, IntegerType, false))
    .add(StructField(RETURN_POINT_WAVEFORM_LOCATION, IntegerType, false))
    .add(StructField(PARAMETRIC_DX, IntegerType, false))
    .add(StructField(PARAMETRIC_DY, IntegerType, false))
    .add(StructField(PARAMETRIC_DZ, IntegerType, false))

}

class LasFilePointParser5 extends LasFilePointParser3{

  override val formatVersion: Int = 5

  override val pointBytesLen: Int = 63

  override val fieldNum: Int = 20

  override def pointSchema: StructType = super.pointSchema
    .add(StructField(WAVE_PACKET_DESCRIPTOR_INDEX, ByteType, false))
    .add(StructField(BYTE_OFFSET_TO_WAVEFORM_DATA, LongType, false))
    .add(StructField(WAVEFORM_PACKET_SIZE_IN_BYTES, IntegerType, false))
    .add(StructField(RETURN_POINT_WAVEFORM_LOCATION, IntegerType, false))
    .add(StructField(PARAMETRIC_DX, IntegerType, false))
    .add(StructField(PARAMETRIC_DY, IntegerType, false))
    .add(StructField(PARAMETRIC_DZ, IntegerType, false))

}

class LasFilePointParser6 extends LasFilePointParser{

  override val formatVersion: Int = 6

  override val pointBytesLen: Int = 30

  override val fieldNum: Int = 10

  override def pointSchema: StructType = StructType(Seq(
    StructField(X, IntegerType, false),
    StructField(Y, IntegerType, false),
    StructField(Z, IntegerType, false),
    StructField(INTENSITY, ShortType, false),
    StructField(RNCSSE, ShortType, false),
    StructField(CLASSFICATION, ByteType, false),
    StructField(USER_DATA, ByteType, false),
    StructField(SCAN_ANGLE, ShortType, false),
    StructField(POINT_SOURCE_ID, ShortType, false),
    StructField(GPS_TIME, LongType, false),
  ))

}

class LasFilePointParser7 extends LasFilePointParser6{

  override val formatVersion: Int = 7

  override val pointBytesLen: Int = 36

  override val fieldNum: Int = 13

  override def pointSchema: StructType = super.pointSchema
    .add(StructField(R, ShortType, false))
    .add(StructField(G, ShortType, false))
    .add(StructField(B, ShortType, false))

}

class LasFilePointParser8 extends LasFilePointParser7{

  override val formatVersion: Int = 8

  override val pointBytesLen: Int = 38

  override val fieldNum: Int = 14

  override def pointSchema: StructType = super.pointSchema
    .add(StructField(NIR, ShortType, false))

}

class LasFilePointParser9 extends LasFilePointParser6{

  override val formatVersion: Int = 9

  override val pointBytesLen: Int = 59

  override val fieldNum: Int = 17

  override def pointSchema: StructType = super.pointSchema
    .add(StructField(WAVE_PACKET_DESCRIPTOR_INDEX, ByteType, false))
    .add(StructField(BYTE_OFFSET_TO_WAVEFORM_DATA, LongType, false))
    .add(StructField(WAVEFORM_PACKET_SIZE_IN_BYTES, IntegerType, false))
    .add(StructField(RETURN_POINT_WAVEFORM_LOCATION, IntegerType, false))
    .add(StructField(PARAMETRIC_DX, IntegerType, false))
    .add(StructField(PARAMETRIC_DY, IntegerType, false))
    .add(StructField(PARAMETRIC_DZ, IntegerType, false))

}

class LasFilePointParser10 extends LasFilePointParser9{

  override val formatVersion: Int = 10

  override val pointBytesLen: Int = 67

  override val fieldNum: Int = 21

  override def pointSchema: StructType = super.pointSchema
    .add(StructField(R, ShortType, false))
    .add(StructField(G, ShortType, false))
    .add(StructField(B, ShortType, false))
    .add(StructField(NIR, ShortType, false))

}

