import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AlphabetCountMapper extends Mapper<IntWritable, Text,IntWritable,Text> {
    public void map(IntWritable key, Text value,Context context) throws IOException, InterruptedException {
        String TextToString = value.toString();
        String[] strarr= TextToString.split(" ");
        for(String s: strarr){
            IntWritable ContextKey = new IntWritable(s.length());
            Text ContextValue= new Text(s);
            context.write(ContextKey , ContextValue);
        }


    }
}
