package driver;

import org.apache.hadoop.util.ToolRunner;
import util.Utils;

/**
 * Created by fanzhe on 2016/10/25.
 */
public class SingleLinearRegressionJobTest {

    public static void main(String[] args) throws Exception {
//        <input> <output> <theta_path> <splitter> <average|weight>
        args = new String[]{
                "hdfs://master:8020/user/fanzhe/shuffle_out",
                "hdfs://master:8020/user/fanzhe/single_linear_regression_error",
                "hdfs://master:8020/user/fanzhe/linear_regression",
                ",",
                "average"
        }    ;
        ToolRunner.run(Utils.getConf(),new SingleLinearRegressionError(),args);
    }
}
