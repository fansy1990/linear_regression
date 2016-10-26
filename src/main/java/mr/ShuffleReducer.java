package mr;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer接受的数据已经被打散，直接输出即可
 * Created by fanzhe on 2016/10/23.
 */
public class ShuffleReducer extends Reducer<FloatWritable,Text,Text,NullWritable> {
    @Override
    protected void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text value: values){
            context.write(value,NullWritable.get());
        }
    }
}
