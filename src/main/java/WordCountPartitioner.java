import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartitioner extends Partitioner<WordCountCompositeKey, Text> {

    private Text newKey = new Text();
    private HashPartitioner hashPartitioner = new HashPartitioner<Text, Text>();

    @Override
    public int getPartition(WordCountCompositeKey wordCountCompositeKey, Text text, int numPartitions) {

        try {
            newKey.set(wordCountCompositeKey.getInputKey());
            return hashPartitioner.getPartition(newKey, text, numPartitions);

        } catch (Exception e) {

            e.printStackTrace();
            return (int) Math.random() * numPartitions;
        }

    }
}
