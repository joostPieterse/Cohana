import java.util.HashMap;
import java.util.Hashtable;

public class BirthSelectionOperator extends Operator{

    public BirthSelectionOperator(Chunk chunk, String birthValue, String birthColumnName, Condition condition) {
        this.chunk = chunk;
        this.birthValue = birthValue;
        this.birthColumnName = birthColumnName;
        this.condition = condition;
    }

    @Override
    public void open() {
        chunk.open();
        currentUser = -1;
    }

    @Override
    public Tuple getNext() {
        Triple user = chunk.getNextUser();
        Tuple nextTuple = chunk.getNext();
        if (nextTuple == null) {
            return null;
        }
        if (nextTuple.user == currentUser) {
            return nextTuple;
        }
        while (user != null) {
            currentUser = user.u;
            Tuple birthTuple = getBirthTuple(nextTuple);

            //some condition on the birth tuple
            if (condition.isBirthTupleQualified(birthTuple)) {
                return nextTuple;
            }
            chunk.skipCurUser();
            user = chunk.getNextUser();
            nextTuple = chunk.getNext();
        }
        return null;
    }

}
