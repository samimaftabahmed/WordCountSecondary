import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WordCountGroup extends WritableComparator {

    public WordCountGroup() {

        super(WordCountCompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        WordCountCompositeKey key1 = (WordCountCompositeKey) a;
        WordCountCompositeKey key2 = (WordCountCompositeKey) b;

        return key1.getInputKey().compareTo(key2.getInputKey());
    }
}
