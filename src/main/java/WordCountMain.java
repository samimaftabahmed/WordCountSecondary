import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCountMain {

    public static void main(String[] args) {

        try {

            Job job = Job.getInstance();
            job.setJobName("Sort Words by value");
            job.setJarByClass(WordCountMain.class);

            job.setMapperClass(WordCountMapper.class);
            job.setReducerClass(WordCountReducer.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setMapOutputKeyClass(WordCountCompositeKey.class);
            job.setMapOutputValueClass(NullWritable.class);

            job.setOutputKeyClass(WordCountCompositeKey.class);
            job.setOutputValueClass(NullWritable.class);

            job.setPartitionerClass(WordCountPartitioner.class);
            job.setGroupingComparatorClass(WordCountGroup.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
        }


    }


}
