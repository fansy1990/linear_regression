package kvtype;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by fanzhe on 2016/10/23.
 */
public class FloatAndFloat implements WritableComparable<FloatAndFloat> {

    private float theta0 ;
    private float theta1;
    public FloatAndFloat(){
        set(0.0f,0.0f);
    }

    public FloatAndFloat(float[] data){
        set(data);
    }

    public void set(float t1,float t2){
        this.theta0 = t1;
        this.theta1 = t2;
    }

    public float getTheta0() {
        return theta0;
    }

    public float getTheta1() {
        return theta1;
    }

    public void set(float[] theta){

        this.theta0 = theta[0];
        this.theta1 =theta[1];
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(theta0);
        dataOutput.writeFloat(theta1);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        theta0 = dataInput.readFloat();
        theta1 = dataInput.readFloat();
    }

    @Override
    public String toString() {
        return this.theta0 +","+ this.theta1;
    }

    @Override
    public int compareTo(FloatAndFloat o) {
        if(this.getTheta0() == o.getTheta0()){
            if(this.getTheta1() <o.getTheta1()) return -1;
            return 1;
        }else if(this.getTheta0()< o.getTheta0()) return -1;

        return 1;
    }
}
