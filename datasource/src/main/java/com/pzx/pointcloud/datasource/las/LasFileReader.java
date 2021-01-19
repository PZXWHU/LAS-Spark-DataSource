package com.pzx.pointcloud.datasource.las;

import com.pzx.pointcloud.datasource.utils.LittleEndianUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

public class LasFileReader implements Closeable, Serializable {

    private FSDataInputStream fileInputStream;

    private volatile LasFileHeader lasFileHeader;

    public LasFileReader(Path path, Configuration configuration) {
        try {
            this.fileInputStream = path.getFileSystem(configuration).open(path);
        }catch (IOException e){
            throw new RuntimeException("init las file failed :" + e.getMessage());
        }

    }

    /**
     * 初始化LasFileHeader
     */
    public LasFileHeader getLasFileHead(){
        if (lasFileHeader == null){
            synchronized (this){
                if (lasFileHeader == null){
                    try {
                        //读取文件版本
                        fileInputStream.seek(LasFileHeader.VERSION_OFFSET);
                        String version = fileInputStream.readByte() + "." + fileInputStream.readByte();
                        //读取文件头大小
                        fileInputStream.seek(LasFileHeader.HEADER_SIZE_OFFSET);
                        int headerSize = LittleEndianUtils.bytesToUnsignedShort(fileInputStream.readByte(), fileInputStream.readByte());
                        //读取文件头
                        fileInputStream.seek(0);
                        byte[] fileHeadBytes = new byte[headerSize];
                        fileInputStream.read(fileHeadBytes);
                        //创建文件头对象
                        lasFileHeader = new LasFileHeader(fileHeadBytes, version);
                    }catch (IOException e){
                        throw new RuntimeException("init las file head failed :" + e.getMessage());
                    }
                }
            }
        }
        return lasFileHeader;
    }

    public synchronized void read(byte[] bytes) {
        try {
            fileInputStream.read(bytes);
        }catch (IOException e){
            throw new RuntimeException("read las file failed :" + e.getMessage());
        }

    }

    public synchronized void read(byte[] bytes, long index) {
        try {
            fileInputStream.seek(index);
            fileInputStream.read(bytes);
        }catch (IOException e){
            throw new RuntimeException("read las file failed :" + e.getMessage());
        }

    }

    public synchronized void seek(long index){
        try {
            fileInputStream.seek(index);
        }catch (IOException e){
            throw new RuntimeException("seek las file failed :" + e.getMessage());
        }

    }

    public void close() throws IOException {
        fileInputStream.close();
    }


}
