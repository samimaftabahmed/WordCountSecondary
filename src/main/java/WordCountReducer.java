import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<WordCountCompositeKey, NullWritable, WordCountCompositeKey, NullWritable> {

    @Override
    protected void reduce(WordCountCompositeKey key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        for (NullWritable vals : values) {

            context.write(key, NullWritable.get());
        }
    }
}
