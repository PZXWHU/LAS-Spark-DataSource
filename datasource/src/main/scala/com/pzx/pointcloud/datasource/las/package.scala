package com.pzx.pointcloud.datasource

import org.apache.spark.sql.{ SQLContext, DataFrameReader, DataFrameWriter, DataFrame }

package object las {
  /**
    * Adds a method, `las`, to DataFrameReader that allows you to read las files using
    * the DataFileReade
    */
  implicit class LasDataFrameReader(reader: DataFrameReader) {
    def las: String => DataFrame = reader.format("com.pzx.pointcloud.datasource.las.LasFileFormat").load
  }


}
