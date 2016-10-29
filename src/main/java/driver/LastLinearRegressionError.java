package driver;

import kvtype.FloatAndFloat;
import kvtype.FloatAndLong;
import mr.SingleLinearRegressionErrorMapper;
import mr.SingleLinearRegressionErrorReducer;
import mr.SingleLinearRegressionErrorReducer2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import util.Utils;

/**
 * 第四步：使用第三步中求得的theta值来求解全局误差
 *
 * Created by fanzhe on 2016/10/23.
 */
public class LastLinearRegressionError extends Configured implements Tool
{
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration() , new LastLinearRegressionError(),args);
    }
    @Override
    public int run(String[] args) throws Exception {
        if(args.length!= 4){
            System.err.println("Usage : driver.LastLinearRegressionError <input> <output> <theta_path> <splitter> " );
            System.exit(-1);
        }
        Configuration conf = getConf();


        conf.set(Utils.SINGLE_LINEAR_PATH,args[2]);
        conf.set(Utils.LINEAR_SPLITTER,args[3]);

        Job job = Job.getInstance(conf,"Last Linear Regression Error");

        job.setMapperClass(SingleLinearRegressionErrorMapper.class);
        job.setReducerClass(SingleLinearRegressionErrorReducer2.class);//

        job.setMapOutputKeyClass(FloatAndFloat.class);
        job.setMapOutputValueClass(FloatAndLong.class);

        job.setOutputKeyClass(FloatAndFloat.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, Utils.str2Path(args[0]));
        FileOutputFormat.setOutputPath(job, Utils.str2Path(args[1]));
        Utils.delete(conf, args[1]);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
