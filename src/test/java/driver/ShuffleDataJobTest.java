package driver;

import org.apache.hadoop.util.ToolRunner;
import util.Utils;

/**
 * Created by fanzhe on 2016/10/25.
 */
public class ShuffleDataJobTest {

    public static void main(String[] args) throws Exception {
        args = new String[]{
                "hdfs://master:8020/user/fanzhe/linear_regression.txt",
                "hdfs://master:8020/user/fanzhe/shuffle_out",
                "1"
        }    ;
        ToolRunner.run(Utils.getConf(),new ShuffleDataJob(),args);

    }
}
