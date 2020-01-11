import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class HadoopStudyTest {
    @Test
    public void mkdirToHdfs() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFs", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.mkdirs(new Path("/kkb/dir1"));
        fileSystem.close();
    }
    @Test
    public void uploadFile() throws IOException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(FileSystem.getDefaultUri(configuration), configuration, "hadoop");
        fileSystem.copyFromLocalFile(new Path("file:///f:\\edit1.txt"), new Path("hdfs://node01:8020/shell"));
        fileSystem.close();
    }
    @Test
    public void downloadFile() throws IOException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(FileSystem.getDefaultUri(configuration), configuration, "hadoop");
        fileSystem.copyToLocalFile(new Path("hdfs://node01:8020/edit1.txt"), new Path("file:///F:\\edit1.txt"));
        fileSystem.close();
    }

    @Test
    public void deleteFileOnExit() throws IOException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(FileSystem.getDefaultUri(configuration), configuration, "hadoop");
        fileSystem.deleteOnExit(new Path("hdfs://node01:8020/shell/CloudHub__1214194040.dmp"));
        fileSystem.close();
    }

    @Test
    public void deleteFile() throws IOException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(FileSystem.getDefaultUri(configuration), configuration, "hadoop");
        fileSystem.delete(new Path("hdfs://node01:8020/test"), true);
        fileSystem.close();
    }

    @Test
    public void changeFileName() throws IOException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(FileSystem.getDefaultUri(configuration), configuration, "hadoop");
        fileSystem.rename(new Path("hdfs://node01:8020/shell/edit1.txt"), new Path("hdfs://node01:8020/shell/edit2.txt"));
        fileSystem.close();
    }
}
