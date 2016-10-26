package mr;

import kvtype.FloatAndFloat;
import kvtype.FloatAndLong;
import org.apache.hadoop.io.FloatWritable;
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
public class SingleLinearRegressionErrorReducer2 extends Reducer<FloatAndFloat,FloatAndLong,FloatAndFloat,FloatWritable> {
    Logger logger = LoggerFactory.getLogger(getClass());


    @Override
    protected void reduce(FloatAndFloat key, Iterable<FloatAndLong> values, Context context) throws IOException, InterruptedException {
        float sumF = 0.0f;
        long sumL = 0L ;
        for(FloatAndLong value:values){
            sumF +=value.getSumFloat();
            sumL += value.getSumLong();
        }

        context.write(key, new FloatWritable((float)Math.sqrt((double)sumF / sumL)));
        logger.info("theta:{}, error:{}", new Object[]{key.toString(),Math.sqrt(sumF/sumL)});
    }
}
