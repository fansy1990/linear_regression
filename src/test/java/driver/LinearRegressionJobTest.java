package driver;

import org.apache.hadoop.util.ToolRunner;
import util.Utils;

/**
 * Created by fanzhe on 2016/10/25.
 */
public class LinearRegressionJobTest {

    public static void main(String[] args) throws Exception {
//        <input> <output> <theta0;theta1;alpha> <splitter>
        args = new String[]{
                "hdfs://master:8020/user/fanzhe/shuffle_out",
                "hdfs://master:8020/user/fanzhe/linear_regression",
                "1,0,0.01",
                ","
        }    ;
        ToolRunner.run(Utils.getConf(),new LinearRegressionJob(),args);

    }
}
