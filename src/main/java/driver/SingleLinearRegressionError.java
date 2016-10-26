package driver;

import kvtype.FloatAndFloat;
import kvtype.FloatAndLong;
import mr.LinearRegressionMapper;
import mr.SingleLinearRegressionErrorMapper;
import mr.SingleLinearRegressionErrorReducer;
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
 * 第三步：使用第二步中随机梯度下降（SGD求解每个mapper的最优函数 来求解全局误差
 * 有多少个mapper就会有多少个全局误差与之对应
 * Reducer： 根据误差来加权(或平均)得到最终的 最优函数
 *
 * Created by fanzhe on 2016/10/23.
 */
public class SingleLinearRegressionError extends Configured implements Tool
{
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration() , new SingleLinearRegressionError(),args);
    }
    @Override
    public int run(String[] args) throws Exception {
        if(args.length!= 5){
            System.err.println("Usage : driver.SingleLinearRegressionError <input> <output> <theta_path> <splitter> <average|weight>" );
            System.exit(-1);
        }
        Configuration conf = getConf();


        conf.set(Utils.SINGLE_LINEAR_PATH,args[2]);
        conf.set(Utils.LINEAR_SPLITTER,args[3]);

        conf.set(Utils.SINGLE_REDUCER_METHOD,args[4]);
        Job job = Job.getInstance(conf);

        job.setMapperClass(SingleLinearRegressionErrorMapper.class);
        job.setReducerClass(SingleLinearRegressionErrorReducer.class);//

        job.setMapOutputKeyClass(FloatAndFloat.class);
        job.setMapOutputValueClass(FloatAndLong.class);

        job.setOutputKeyClass(FloatAndFloat.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, Utils.str2Path(args[0]));
        FileOutputFormat.setOutputPath(job, Utils.str2Path(args[1]));
        Utils.delete(conf, args[1]);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
