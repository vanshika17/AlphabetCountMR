import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class AlphabetCountReducer extends Reducer<IntWritable, Text,IntWritable, LongWritable> {
    LongWritable ContextValue = new LongWritable();
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        AtomicInteger sum= new AtomicInteger();
        for (Text val:values){
            sum.set(sum.get() + 1);
        }
        ContextValue.set(sum.get());
        context.write(key,ContextValue);
    }
}