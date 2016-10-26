package mr;

import kvtype.FloatAndFloat;
import kvtype.FloatAndLong;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 得到每个theta的参数值的全局误差
 * Created by fanzhe on 2016/10/23.
 */
public class SingleLinearRegressionErrorMapper extends Mapper<LongWritable,Text, FloatAndFloat, FloatAndLong> {

    private String thetaPath = null;
    private String splitter = ",";
    private  List<float[]> thetas = new ArrayList<>();
    private float[] thetaErrors = null;
    private long [] thetaNumbers = null;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        thetaPath = context.getConfiguration().get(Utils.SINGLE_LINEAR_PATH,null);
        splitter = context.getConfiguration().get(Utils.LINEAR_SPLITTER,",");
        if(thetaPath == null) {System.err.println("theta path exception");System.exit(-1);}


        FileStatus[] files = FileSystem.get(context.getConfiguration()).listStatus(Utils.str2Path(thetaPath), new PathFilter() {
            @Override
            public boolean accept(Path path) {
                if(path.toString().contains(Utils.MAPPER_OUTPUT_PREFIX)) return true ;
                return false;
            }
        });

        for(FileStatus file : files){
            thetas.add(Utils.readFromOneTheatFile(context.getConfiguration(), file.getPath(), splitter));
        }
        thetaErrors = new float[thetas.size()];
        thetaNumbers = new long[thetas.size()];
        System.out.println("thetas array size :"+thetas.size());
    }
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        float[] xy = Utils.str2float(value.toString().split(splitter));
        for(int i =0;i<thetas.size() ;i++){
            // error = (theta0 + theta1 * x - y) ^2
            thetaErrors[i] += (thetas.get(i)[0]+ thetas.get(i)[1] * xy[0] -xy[1]) *
                    (thetas.get(i)[0]+ thetas.get(i)[1] * xy[0] -xy[1]) ;
            thetaNumbers[i]+= 1;
        }
    }

    private FloatAndLong floatAndLong = new FloatAndLong();
    private FloatAndFloat theta = new FloatAndFloat();
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(int i =0;i<thetas.size() ;i++){
            theta.set(thetas.get(i));
            floatAndLong.set(thetaErrors[i],thetaNumbers[i]);
            context.write(theta,floatAndLong);
        }
    }
}
