import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.logging.Logger;

/**
 * Created by 전제현 on 2017. 6. 14.
 */
public class HDFSReaderTest {

    private static final Logger logger = Logger.getLogger("HDFSReaderTest");

    public static void main(String[] args) throws Exception {

        Configuration config = new Configuration();
        config.set("fs.defaultFS", "hdfs://192.168.1.67:9000/");
        FileSystem hdfs = FileSystem.get(config);
        Path srcPath = new Path("/user/jhjeon/hadoop/capacity-scheduler.xml");
        Path dstPath = new Path("/Users/jhjeon/Documents/");
        hdfs.copyToLocalFile(srcPath, dstPath);

        //HDFS URI
//
//
//        if (args.length<1) {
//            /*
//            logger.severe("1 arg is required :\n\t- hdfsmasteruri (8020 port) ex: hdfs://namenodeserver:8020");
//            System.err.println("1 arg is required :\n\t- hdfsmasteruri (8020 port) ex: hdfs://namenodeserver:8020");
//            System.exit(128);
//            */
//        }
//
//        //String hdfsuri = args[0];
//        String hdfsuri = "hdfs://192.168.1.39:9000/";
//
//        String path = "/user/hadoop/";
//        String fileName = "test.csv";
//
//        // ====== Init HDFS File System Object
//        Configuration conf = new Configuration();
//        // Set FileSystem URI
//        conf.set("fs.defaultFS", hdfsuri);
//        conf.set("dfs.datanode.address", "192.168.1.39:50010");
//        // Because of Maven
//        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
//        // Set HADOOP user
//        System.setProperty("HADOOP_USER_NAME", "hdfs");
//        System.setProperty("hadoop.home.dir", "/");
//        // Get the filesystem - HDFS
//        FileSystem fs = FileSystem.get(URI.create(hdfsuri), conf);
//
//        // ==== Create folder if not exists
//        Path workingDir = fs.getWorkingDirectory();
//        Path newFolderPath = new Path(path);
//        if(!fs.exists(newFolderPath)) {
//            // Create new Directory
//            fs.mkdirs(newFolderPath);
//            logger.info("Path "+path+" created.");
//        }
//
//        // ==== Read file
//        logger.info("Read file into hdfs");
//        // Create a path
//        Path hdfsreadpath = new Path(newFolderPath + "/" + fileName);
//        // Init input stream
//        FSDataInputStream inputStream = fs.open(hdfsreadpath);
//        // Classical input stream usage
//        String out = IOUtils.toString(inputStream, "UTF-8");
//        logger.info(out);
//        inputStream.close();
//        fs.close();

        /*
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://192.168.1.39:9000/");
            conf.set("dfs.datanode.address", "192.168.1.39:50010");
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] fsStatus = fs.listStatus(new Path("/user/jhjeon/"));
            for (int cnt = 0; cnt < fsStatus.length; cnt++) {
                FileStatus status = fsStatus[cnt];
                //System.out.println(status.getPath());
                Path filePath = status.getPath();
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(filePath)));
                String line;
                line = br.readLine();
                while (line != null) {
                    //System.out.println(line);
                    line = br.readLine();
                }
                System.out.println(line);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.out.println(e.getCause());
        }
        */
    }
}
