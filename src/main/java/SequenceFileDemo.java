import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;


public class SequenceFileDemo {
    private static final String[] DATA = {
            "One, two, buckle my shoe",
            "Three, four, shut the door",
            "Five, six, pick up sticks",
            "Seven, eight, lay them straight",
            "Nine, ten, a big fat hen"
    };

    public static void main(String[] args) throws IOException {
        long startTime = System.currentTimeMillis();


        writeSequenceFile(args);

        //readSequenceFile(args);

        long endTime = System.currentTimeMillis();
        long timeSpan = endTime-startTime;
        System.out.println("总共耗时："+timeSpan+"毫秒");
    }


    private static void writeSequenceFile(String[] args) throws IOException{
        String uri = args[0];
        Configuration conf = new Configuration();
        Path path = new Path(uri);


        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = null;

        SequenceFile.Writer.Option option1 = SequenceFile.Writer.file(path);
        SequenceFile.Writer.Option option2 = SequenceFile.Writer.keyClass(key.getClass());
        SequenceFile.Writer.Option option3 = SequenceFile.Writer.valueClass(value.getClass());
        SequenceFile.Writer.Option option4 = SequenceFile.Writer.compression(SequenceFile.CompressionType.RECORD);
        try {
            writer = SequenceFile.createWriter(conf,option1,option2,option3,option4);
            for (int i=0;i<100;i++){
                key.set(i+1);
                value.set(DATA[i% DATA.length]);
                System.out.printf("[%s]\t%s\t%s\n",writer.getLength(), key,value);
                writer.append(key,value);
                if(i%DATA.length==0)
                    writer.sync();//写入同步点
            }
        }finally {
            IOUtils.closeStream(writer);
        }
    }

    private static void readSequenceFile(String[] args) throws IOException{
        String uri = args[0];
        Configuration conf = new Configuration();
        Path path = new Path(uri);

        SequenceFile.Reader reader = null;
        SequenceFile.Reader.Option option1 = SequenceFile.Reader.file(path);
        try {
            reader = new SequenceFile.Reader(conf,option1);
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(),conf);
            Writable value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(),conf);
            long position = reader.getPosition();
            while (reader.next(key,value)){
                String syncSeen = reader.syncSeen() ? "*":"";//同步位显示为*号
                System.out.printf("[%s%s]\t%s\t%s\n",position,syncSeen,key,value);
                position = reader.getPosition();
            }
        }finally {
            IOUtils.closeStream(reader);
        }
    }
}
