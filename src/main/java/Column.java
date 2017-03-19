import java.util.ArrayList;
import java.util.BitSet;

public class Column {
    private BitSet bits;
    private int lengthPerValue;
    private int numValues;

    public Column(ArrayList<Integer> plainValues) {
        numValues = plainValues.size();
        int maximum = 0;
        for (int val : plainValues) {
            maximum = Math.max(maximum, val);
        }
        if (maximum == 0) {
            lengthPerValue = 1;
        } else {
            lengthPerValue = (int) (Math.log(maximum) / Math.log(2)) + 1;
        }
        bits = new BitSet(lengthPerValue * plainValues.size());
        for (int i = 0; i < plainValues.size(); i++) {
            int val = plainValues.get(i);
            for (int j = lengthPerValue - 1; j >= 0; j--) {
                bits.set(i * lengthPerValue + lengthPerValue - j - 1, ((1 << j) & val) > 0);
            }
        }
    }

    //minimum value needs to be added to this for integer columns
    public int get(int index) {
        if (index < 0 || index >= numValues) {
            throw new IndexOutOfBoundsException("index: " + index + " length: " + numValues);
        }
        int result = 0;
        for (int i = 0; i < lengthPerValue; i++) {
            if (bits.get(lengthPerValue * index + i)) {
                result += 1 << (lengthPerValue - i - 1);
            }
        }
        return result;
    }

}
