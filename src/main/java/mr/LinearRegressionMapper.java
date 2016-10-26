package mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.Utils;

import java.io.IOException;

/**
 * 每个mapper的map函数更新 theta0 和 theta1的公式为
 * theta0 = theta0 - alpha *( h(x) - y) * x
 * theta1 = theta1 - alpha *( h(x) - y) * x
 *
 * Created by fanzhe on 2016/10/23.
 */
public class LinearRegressionMapper extends Mapper<LongWritable,Text,Text,NullWritable> {

    private float theta0 =1.0f ;
    private float theta1 =0.0f ;
    private float alpha = 0.01f;
    private String splitter = ",";

    private float lastTheta0 = theta0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        theta0 = context.getConfiguration().getFloat(Utils.LINEAR_THETA0,1.0f);
        theta1 = context.getConfiguration().getFloat(Utils.LINEAR_THETA1,0.0f);
        alpha = context.getConfiguration().getFloat(Utils.LINEAR_ALPHA,0.01f);
        splitter = context.getConfiguration().get(Utils.LINEAR_SPLITTER,",");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        float[] xy = Utils.str2float(value.toString().split(splitter));
        float x = xy[0];
        float y = xy[1];
        // 同步更新 theta0 and theta1
        lastTheta0 = theta0;

        theta0 -=  alpha *(theta0+theta1* x - y) * x; // 保持theta0 和theta1 不变
        theta1 -= alpha *(lastTheta0 + theta1 * x -y) * x;// 保持theta0 和theta1 不变
    }

    private Text theta0_1 = new Text();
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        theta0_1.set(theta0 + splitter + theta1);
        context.write(theta0_1,NullWritable.get());
    }
}
