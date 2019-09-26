import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordCountCompositeKey implements WritableComparable<WordCountCompositeKey> {


    private IntWritable inputCount = new IntWritable();
    private Text inputKey = new Text();


    public WordCountCompositeKey() {

    }

    public WordCountCompositeKey(IntWritable inputCount, Text inputKey) {
        this.inputCount.set(inputCount.get());
        this.inputKey.set(inputKey);
    }

    @Override
    public int compareTo(WordCountCompositeKey wordCountCompositeKey) {

        return ComparisonChain.start()
                .compare(inputCount, wordCountCompositeKey.inputCount)
                .compare(inputKey, wordCountCompositeKey.inputKey)
                .result();

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WordCountCompositeKey that = (WordCountCompositeKey) o;
        return Objects.equal(inputCount, that.inputCount) &&
                Objects.equal(inputKey, that.inputKey);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(inputCount, inputKey);
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
