import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordCountCompositeKey implements WritableComparable<WordCountCompositeKey> {


    private IntWritable inputCount;
    private Text inputKey;


    public WordCountCompositeKey() {

        this.inputCount = new IntWritable();
        this.inputKey = new Text();
    }

    public WordCountCompositeKey(IntWritable inputCount, Text inputKey) {
        this.inputCount = inputCount;
        this.inputKey = inputKey;
    }

    @Override
    public int compareTo(WordCountCompositeKey wordCountCompositeKey) {

        int value = inputKey.compareTo(wordCountCompositeKey.getInputKey());

        if (value != 0) {
            return -inputCount.compareTo(wordCountCompositeKey.getInputCount());
        }

        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        inputCount.write(out);
        inputKey.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        inputCount.readFields(in);
        inputKey.readFields(in);
    }

    // getters and setters

    public IntWritable getInputCount() {
        return inputCount;
    }

    public void setInputCount(IntWritable inputCount) {
        this.inputCount = inputCount;
    }

    public Text getInputKey() {
        return inputKey;
    }

    public void setInputKey(Text inputKey) {
        this.inputKey = inputKey;
    }

    @Override
    public String toString() {
        return "WordCountCompositeKey{" +
                "  inputCount=" + inputCount +
                ", inputKey=" + inputKey +
                '}';
    }
}
