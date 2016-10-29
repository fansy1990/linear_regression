package driver;

import mr.ShuffleMapper;
import mr.ShuffleReducer;
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
 * 第一步：打乱数据
 * 方法：
 * Mapper端输出随机值作为key，在reducer端直接输出即可
 * randN : 间隔多久生成一次随机数
 * Created by fanzhe on 2016/10/23.
 */
public class ShuffleDataJob extends Configured implements Tool
{
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new ShuffleDataJob(), args);
    }
    @Override
    public int run(String[] args) throws Exception {
        if(args.length!= 3){

            System.err.println("Usage : driver.ShuffleDataJob <input> <output> <randN>");
            System.exit(-1);
        }
        Configuration conf = getConf();
        int randN = 0;
        try{
            randN = Integer.parseInt(args[2]);
        }catch (NumberFormatException e){
            e.printStackTrace();
        }
        conf.setInt(Utils.SHUFFLE_RANDN, randN);

        Job job = Job.getInstance(conf,"Shuffle Data Job");
        job.setJarByClass(ShuffleDataJob.class);

        job.setMapperClass(ShuffleMapper.class);
        job.setReducerClass(ShuffleReducer.class);

        job.setMapOutputKeyClass(FloatWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, Utils.str2Path(args[0]));
        FileOutputFormat.setOutputPath(job,Utils.str2Path(args[1]));

        Utils.delete(conf,args[1]);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
