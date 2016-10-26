package driver;

import org.apache.hadoop.util.ToolRunner;
import util.Utils;

/**
 * Created by fanzhe on 2016/10/25.
 */
public class LastLinearRegressionJobTest {

    public static void main(String[] args) throws Exception {
//        <input> <output> <theta_path> <splitter>
        args = new String[]{
                "hdfs://master:8020/user/fanzhe/shuffle_out",
                "hdfs://master:8020/user/fanzhe/last_linear_regression_error",
                "hdfs://master:8020/user/fanzhe/single_linear_regression_error",
                ",",
        }    ;
        ToolRunner.run(Utils.getConf(),new LastLinearRegressionError(),args);

    }
}
