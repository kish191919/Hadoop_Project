import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class ListStatus {

    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);


        Path path = new Path(uri);
        FileStatus[] status = fs.listStatus(path);
        // FileStatus 정보 : HdfsNamedFileStatus{path=hdfs://localhost:9000/user/LICENSE.txt; isDirectory=false; length=15217; replication=1; blocksize=134217728; modification_time=1658345779809; access_time=1658345779658; owner=sunghwanki; group=supergroup; permission=rw-r--r--; isSymlink=false; hasAcl=false; isEncrypted=false; isErasureCoded=false}

        // Status 정보에서 file path로 변환
        Path[] listPaths = FileUtil.stat2Paths(status);
        for (Path p : listPaths){
            System.out.println(p);
        }
    }
}
