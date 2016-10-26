package mr;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.Utils;

import java.io.IOException;
import java.util.Random;

/**
 * Created by fanzhe on 2016/10/23.
 */
public class ShuffleMapper extends Mapper<LongWritable,Text,FloatWritable,Text> {

    private Random random = new Random();

    private int randN =0;

    private int countI =0;

    private FloatWritable randFloatKey = new FloatWritable(random.nextFloat());
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        randN = context.getConfiguration().getInt(Utils.SHUFFLE_RANDN,0);

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(randN <= 0) { // 如果randN 比0小，那么不再次打乱数据
            context.write(randFloatKey,value);
            return ;
        }
        if(++countI >= randN){// 如果randN等于1，那么每次随机的值都是不一样的
            randFloatKey.set(random.nextFloat());
            countI =0;
        }
        context.write(randFloatKey,value);
    }
}
