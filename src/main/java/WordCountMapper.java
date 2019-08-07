import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper extends Mapper<LongWritable, Text, WordCountCompositeKey, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        StringTokenizer stringTokenizer = new StringTokenizer(value.toString(), "\n");

        while (stringTokenizer.hasMoreTokens()) {

            String[] split = stringTokenizer.nextToken().split("\\t");

            WordCountCompositeKey wordCountCompositeKey = new WordCountCompositeKey(
                    new IntWritable(Integer.parseInt(split[1])), new Text(split[0])
            );

            context.write(wordCountCompositeKey, NullWritable.get());
        } // while ends
    } // func ends
} // class ends
