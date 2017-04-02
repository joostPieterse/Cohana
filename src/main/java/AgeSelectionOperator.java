
public class AgeSelectionOperator extends Operator{

    public AgeSelectionOperator(Chunk chunk, String birthValue, String birthColumnName, Condition condition) {
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
        while (nextTuple.user == currentUser) {
            //some condition on the age tuple
            if (condition.isAgeTupleQualified(nextTuple)) {
                return nextTuple;
            }
            nextTuple = chunk.getNext();
            if (nextTuple == null) {
                return null;
            }
        }
        if (user != null) {
            currentUser = user.u;
            return getBirthTuple(nextTuple);
        }
        return null;
    }

}
