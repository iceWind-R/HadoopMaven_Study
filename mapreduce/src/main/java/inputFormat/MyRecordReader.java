package inputFormat;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MyRecordReader extends RecordReader<NullWritable, BytesWritable> {

    private Configuration configuration = null;
    private FileSplit fileSplit = null;
    private boolean processed = false;  // 标志位，代表文件是否读取完
    private BytesWritable bytesWritable = new BytesWritable();
    private FileSystem fileSystem = null;
    private FSDataInputStream inputStream = null;

    // 进行初始化工作
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        // 获取COnfiguration对象
        configuration = taskAttemptContext.getConfiguration();
        // 获取文件的切片
        fileSplit = (FileSplit) inputSplit;
    }

    // 该方法用于获取 K1 和 V1
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!processed) {
            // 获取源文件的字节输入流
            // 获取源文件的文件系统 (FileSystem)
            fileSystem = FileSystem.get(configuration);
            // 通过FileSystem获取文件输入流
            inputStream = fileSystem.open(fileSplit.getPath());

            // 读取数据到普通的 字节数据
            byte[] bytes = new byte[(int) fileSplit.getLength()]; // 根据每个小文件的大小指定字节数组长度
            IOUtils.readFully(inputStream, bytes, 0, (int) fileSplit.getLength());
            // 将字节数组封装到 BytesWritable，得到 V1

            bytesWritable.set(bytes, 0, (int) fileSplit.getLength());

            processed = true;
            return true;
        }
        return false;
    }

    // 返回 K1
    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    // 返回 V1
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
    }

    // 获取文件读取的进度
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    // 进行资源释放
    @Override
    public void close() throws IOException {
        inputStream.close();
        fileSystem.close();
    }
}
