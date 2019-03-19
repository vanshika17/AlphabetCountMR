import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AplhabetCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration=new Configuration();
        Job job= new Job(configuration);
        job.setNumReduceTasks(3);
        job.setMapperClass(AlphabetCountMapper.class);
        job.setReducerClass(AlphabetCountReducer.class);
        //key type as intwriteable
        job.setOutputKeyClass(IntWritable.class);
        //value type as longwriteable
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job,new Path(args[1]));
        //take output file path
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        //if output file already exists then delete existing
        FileSystem fileSystem= FileSystem.get(configuration);
        fileSystem.delete(new Path(args[2]));
        //wait for completion
        System.exit( job.waitForCompletion(true)?0:1);
    }
}
