package com.pzx.pointcloud.datasource.las;

import com.pzx.pointcloud.datasource.utils.LittleEndianUtils;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * LAS文件的header部分，目前并没有读取header中的全部信息，只是读取部分重要信息
 * 兼容1.0-1.4
 */
public class LasFileHeader implements Serializable {

    public static final int VERSION_OFFSET = 24;
    public static final int HEADER_SIZE_OFFSET = 94;

    private byte[] buffer;
    private int position;

    private String version;
    private int headerSize;
    private long offsetToPointData;
    private long numberOfVariableLengthRecords;
    private byte pointDataFormatID;
    private int pointDataRecordLength;
    private long numberOfPointRecords;
    private double xScale;
    private double yScale;
    private double zScale;
    private double xOffset;
    private double yOffset;
    private double zOffset;
    private double maxX;
    private double maxY;
    private double maxZ;
    private double minX;
    private double minY;
    private double minZ;

    public LasFileHeader(byte[] buffer, String version){
        this.buffer = buffer;
        this.headerSize = buffer.length;
        this.version = version;
        this.position = 96;//略过前面不需要读取的信息

        offsetToPointData = LittleEndianUtils.bytesToUnsignedInteger(readBytes(4));
        numberOfVariableLengthRecords = LittleEndianUtils.bytesToUnsignedInteger(readBytes(4));
        pointDataFormatID = readBytes(1)[0];
        pointDataRecordLength = LittleEndianUtils.bytesToUnsignedShort(readBytes(2));
        numberOfPointRecords = LittleEndianUtils.bytesToUnsignedInteger(readBytes(4));

        if(Float.parseFloat(version)>=1.3)
            position += 28;// version >= 1.3 Number of points by return 28bytes
        else
            position += 20;//version <= 1.2 Number of points by return 20bytes

        xScale = Math.abs(LittleEndianUtils.bytesToDouble(readBytes(8)));
        yScale = Math.abs(LittleEndianUtils.bytesToDouble(readBytes(8)));
        zScale = Math.abs(LittleEndianUtils.bytesToDouble(readBytes(8)));

        xOffset = LittleEndianUtils.bytesToDouble(readBytes(8));
        yOffset = LittleEndianUtils.bytesToDouble(readBytes(8));
        zOffset = LittleEndianUtils.bytesToDouble(readBytes(8));

        maxX = LittleEndianUtils.bytesToDouble(readBytes(8));
        minX = LittleEndianUtils.bytesToDouble(readBytes(8));
        maxY = LittleEndianUtils.bytesToDouble(readBytes(8));
        minY = LittleEndianUtils.bytesToDouble(readBytes(8));
        maxZ = LittleEndianUtils.bytesToDouble(readBytes(8));
        minZ = LittleEndianUtils.bytesToDouble(readBytes(8));


        //System.out.println(toString());
    }

    private byte[] readBytes(int size){
        byte[] bytes = Arrays.copyOfRange(buffer, position, position + size);
        position += size;
        return bytes;
    }

    public long getOffsetToPointDataEnd(){
        return offsetToPointData + pointDataRecordLength * numberOfPointRecords - 1;
    }

    public int getHeaderSize() {
        return headerSize;
    }

    public long getOffsetToPointData() {
        return offsetToPointData;
    }

    public long getNumberOfVariableLengthRecords() {
        return numberOfVariableLengthRecords;
    }

    public byte getPointDataFormatID() {
        return pointDataFormatID;
    }

    public int getPointDataRecordLength() {
        return pointDataRecordLength;
    }

    public long getNumberOfPointRecords() {
        return numberOfPointRecords;
    }

    public double getxScale() {
        return xScale;
    }

    public double getyScale() {
        return yScale;
    }

    public double getzScale() {
        return zScale;
    }

    public double getxOffset() {
        return xOffset;
    }

    public double getyOffset() {
        return yOffset;
    }

    public double getzOffset() {
        return zOffset;
    }

    public double getMaxX() {
        return maxX;
    }

    public double getMaxY() {
        return maxY;
    }

    public double getMaxZ() {
        return maxZ;
    }

    public double getMinX() {
        return minX;
    }

    public double getMinY() {
        return minY;
    }

    public double getMinZ() {
        return minZ;
    }


    @Override
    public String toString() {
        return "LasFileHeader{" +
                "version='" + version + '\'' +
                ", headerSize=" + headerSize +
                ", offsetToPointData=" + offsetToPointData +
                ", numberOfVariableLengthRecords=" + numberOfVariableLengthRecords +
                ", pointDataFormatID=" + pointDataFormatID +
                ", pointDataRecordLength=" + pointDataRecordLength +
                ", numberOfPointRecords=" + numberOfPointRecords +
                ", xScale=" + xScale +
                ", yScale=" + yScale +
                ", zScale=" + zScale +
                ", xOffset=" + xOffset +
                ", yOffset=" + yOffset +
                ", zOffset=" + zOffset +
                ", maxX=" + maxX +
                ", maxY=" + maxY +
                ", maxZ=" + maxZ +
                ", minX=" + minX +
                ", minY=" + minY +
                ", minZ=" + minZ +
                '}';
    }
}
