import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.logging.Logger;

/**
 * Created by 전제현 on 2017. 6. 14.
 */
public class HDFSReaderTest {

    private static final Logger logger = Logger.getLogger("HDFSReaderTest");
    private static final String DEFAULTFS = "fs.defaultFS";

    public static void main(String[] args) throws Exception {

        String HDFSDefaultFS = args[0];
        String fileRootDirectoryPath = args[1];

        Configuration conf = new Configuration();
        conf.set(DEFAULTFS, HDFSDefaultFS);
        Path filePath = new Path(fileRootDirectoryPath);
        readHDFSFiles(conf, filePath);
    }

    private static void readHDFSFiles(Configuration conf, Path filePath) throws Exception {
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fsStatus = fs.listStatus(filePath);
        try {
            for (int cnt = 0; cnt < fsStatus.length; cnt++) {
                FileStatus status = fsStatus[cnt];
                //System.out.println(status.getPath());
                if (status.isDirectory()) {
                    // 여기서 하위 디렉토리의 파일들의 리스트를 다시 가져와서 파일 내용을 읽어야 한다.
                    readHDFSFiles(conf, status.getPath());
                } else {
                    Path path = status.getPath();
                    BufferedReader br = null;
                    try {
                        br = new BufferedReader(new InputStreamReader(fs.open(path)));
                        /*
                        String line;
                        System.out.println(status.getPath().getName());
                        System.out.println("=====================================================");
                        line = br.readLine();
                        while (line != null) {
                            System.out.println(line);
                            if (line != null) {
                                line = br.readLine();
                            }
                        }
                        if (line != null) {
                            System.out.println(line);
                        }
                        System.out.println("=====================================================");
                        */
                    } catch (Exception e) {

                    } finally {
                        br.close();
                    }
                }
            }
        } catch (Exception e) {

        } finally {
            fs.close();
        }
    }
}
