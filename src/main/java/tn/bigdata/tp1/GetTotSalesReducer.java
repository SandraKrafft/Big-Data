package tn.bigdata.tp1;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class GetTotSalesReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private FloatWritable result = new FloatWritable();
    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float sum = 0;
        for (FloatWritable val : values) {
            System.out.println("value: " + val.get());
            sum += val.get();
        }
        System.out.println("--> Sum = " + sum);
        System.out.println("--> Sum = " + sum);
        result.set(sum);
        context.write(key, result);
    }
}
