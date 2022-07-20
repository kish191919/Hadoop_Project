import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class FileSystemPrint{

    public static void main(String[] args) throws IOException {

        // 파일위치를 인수로 받음
        String uri = args[0];
        // 하둡과 관련된 conf 생성
        Configuration conf = new Configuration();
        // Filesystem.get 같은 경우는 IOException이 발생할수 있기 때문에 Exception을 선언
        FileSystem fs = FileSystem.get(URI.create(uri),conf);
        try (InputStream in = fs.open(new Path(uri))){
            IOUtils.copyBytes(in, System.out, 4096,false);
        }
    }
}