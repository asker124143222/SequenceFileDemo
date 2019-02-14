import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.http.util.Args;
import sun.security.util.Length;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;


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


//        writeSequenceFile(args);

        //readSequenceFile(args);

//        List<String> list = getFiles(args[0]);
//        for (int i=0;i<list.size();i++)
//        {
//            System.out.println(list.get(i));
//        }

        combineToSequenceFile(args);
        long endTime = System.currentTimeMillis();
        long timeSpan = endTime-startTime;
        System.out.println("总共耗时："+timeSpan+"毫秒");
    }


    //将目标目录的所有文件以文件名为key，内容为value放入SequenceFile中
    //第一个参数是需要打包的目录，第二个参数生成的文件路径和名称
    private static void combineToSequenceFile(String[] args) throws IOException{
        String sourceDir = args[0];
        String destFile = args[1];

        List<String> files = getFiles(sourceDir);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path destPath = new Path(destFile);
        if(fs.exists(destPath))
        {
            fs.delete(destPath,true);
        }

        FSDataInputStream in = null;

        Text key = new Text();
        BytesWritable value = new BytesWritable();

        SequenceFile.Writer writer = null;

        SequenceFile.Writer.Option option1 = SequenceFile.Writer.file(new Path(destFile));
        SequenceFile.Writer.Option option2 = SequenceFile.Writer.keyClass(key.getClass());
        SequenceFile.Writer.Option option3 = SequenceFile.Writer.valueClass(value.getClass());
        SequenceFile.Writer.Option option4 = SequenceFile.Writer.compression(SequenceFile.CompressionType.RECORD);
        try {
            writer = SequenceFile.createWriter(conf,option1,option2,option3,option4);
            for(int i=0;i<files.size();i++)
            {
                Path path = new Path(files.get(i).toString());
                System.out.println("读取文件："+path.toString());
                key = new Text(files.get(i).toString());
                in = fs.open(path);
//                只能处理小文件，int最大只能表示到1个G的大小
                int length = (int)fs.getFileStatus(path).getLen();
                byte[] buff = new byte[length];
//                read最多只能读取65536的大小，这里还有bug需要修复
                int readLength = in.read(buff);
                System.out.println("file length:"+length+",read length:"+readLength);
                value = new BytesWritable(buff,readLength);
                System.out.printf("[%s]\t%s\t%s\n",writer.getLength(), key,value.getLength());
                writer.append(key,value);
            }
        }finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(writer);
            IOUtils.closeStream(fs);
        }

    }

    private static List<String> getFiles(String dir) throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path(dir);
        FileSystem fs = null;
        List<String> filelist = new ArrayList<>();
        try {
            fs = FileSystem.get(conf);

            //对单个文件或目录下所有文件和目录
            FileStatus[] fileStatuses = fs.listStatus(path);

            for (FileStatus fileStatus : fileStatuses) {
                //递归查找子目录
                if (fileStatus.isDirectory()) {
                    filelist.addAll(getFiles(fileStatus.getPath().toString()));
                }
                else{
                    filelist.add(fileStatus.getPath().toString());
                }
            }
            return filelist;
        } finally {
            IOUtils.closeStream(fs);
        }
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
