
public abstract class Operator {
    protected Chunk chunk;
    protected String birthValue;
    protected String birthColumnName;
    protected Condition condition;
    protected int currentUser;

    protected Tuple getBirthTuple(Tuple firstTuple) {
        Tuple tuple = firstTuple;
        while (tuple.user == currentUser && !birthValue.equals(tuple.stringValues.get(birthColumnName))) {
            tuple = chunk.getNext();
            if (tuple == null) {
                return null;
            }
        }
        return tuple;
    }
    public abstract void open();
    public abstract Object getNext();
}
