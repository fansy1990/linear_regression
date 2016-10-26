package mr;

import kvtype.FloatAndFloat;
import kvtype.FloatAndLong;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by fanzhe on 2016/10/23.
 */
public class SingleLinearRegressionErrorReducer extends Reducer<FloatAndFloat,FloatAndLong,FloatAndFloat,NullWritable> {
    List<float[]> theta_error = new ArrayList<>();
    String method = "average";
    Logger logger = LoggerFactory.getLogger(getClass());
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        method = context.getConfiguration().get(Utils.SINGLE_REDUCER_METHOD);
    }

    @Override
    protected void reduce(FloatAndFloat key, Iterable<FloatAndLong> values, Context context) throws IOException, InterruptedException {
        float sumF = 0.0f;
        long sumL = 0L ;
        for(FloatAndLong value:values){
            sumF +=value.getSumFloat();
            sumL += value.getSumLong();
        }
        theta_error.add(new float[]{key.getTheta0(),key.getTheta1(), (float)Math.sqrt((double)sumF / sumL)});
        logger.info("theta:{}, error:{}", new Object[]{key.toString(),Math.sqrt(sumF/sumL)});
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 如何加权？
        // 方式1：如果误差越小，那么说明权重应该越大；
        // 方式2：直接平均值
        float [] theta_all = null;
        if("average".equals(method)){
            theta_all = theta_error.get(0);
            for(int i=1;i< theta_error.size();i++){
                theta_all[0] += theta_error.get(i)[0];
                theta_all[1] += theta_error.get(i)[1];
            }
            theta_all[0] /= theta_error.size();
            theta_all[1] /= theta_error.size();
        } else {
            float sumErrors = 0.0f;
            for(float[] d:theta_error){
                sumErrors += 1/d[2];
            }

            for(float[] d: theta_error){
                theta_all[0] += d[0] * 1/d[2] /sumErrors;
                theta_all[1] += d[1] * 1/d[2] /sumErrors;
            }
        }
        context.write(new FloatAndFloat(theta_all),NullWritable.get());
    }
}
