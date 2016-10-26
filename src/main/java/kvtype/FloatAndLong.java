package kvtype;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by fanzhe on 2016/10/23.
 */
public class FloatAndLong implements Writable {

    public long getSumLong() {
        return sumLong;
    }

    public void setSumLong(long sumLong) {
        this.sumLong = sumLong;
    }

    public float getSumFloat() {
        return sumFloat;
    }

    public void setSumFloat(float sumFloat) {
        this.sumFloat = sumFloat;
    }

    private float sumFloat ;//全局误差
    private long sumLong;// 个数
    public FloatAndLong(){
        set(0.0f,0L);
    }

    public void set(float sum1,long sum2){
        this.sumFloat = sum1;
        this.sumLong = sum2;
    }
    public float getRMSE(){
        return this.sumFloat/ this.sumLong ;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(sumFloat);
        dataOutput.writeLong(sumLong);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        sumFloat = dataInput.readFloat();
        sumLong = dataInput.readLong();
    }

    @Override
    public String toString() {
        return this.sumFloat+","+this.sumLong;
    }
}
