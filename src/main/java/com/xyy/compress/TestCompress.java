package com.xyy.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

public class TestCompress {

    public static void main(String[] args) throws IOException, ClassNotFoundException {

        //compress("D:\\mrworkspace\\a.log","org.apache.hadoop.io.compress.BZip2Codec");
        decompress("D:\\mrworkspace\\a.log.bz2");
    }

    private static void decompress(String path) throws IOException {
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());

        CompressionCodec codec = factory.getCodec(new Path(path));

        if(codec == null){
            return;
        }

        CompressionInputStream cis = codec.createInputStream(new FileInputStream(new File(path)));


        FileOutputStream fos = new FileOutputStream(new File(path + ".decoded"));

        IOUtils.copyBytes(cis,fos,new Configuration(),true);


    }

    private static void compress(String path, String compressClass) throws ClassNotFoundException, IOException {

        FileInputStream fis = new FileInputStream(new File(path));


        Class<?> codecClass = Class.forName(compressClass);

        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, new Configuration());

        FileOutputStream fos = new FileOutputStream(new File(path+ codec.getDefaultExtension() ));

        CompressionOutputStream cos = codec.createOutputStream(fos);

        IOUtils.copyBytes(fis,cos,new Configuration(),true);


        cos.close();
        fis.close();
        fos.close();

    }
}
