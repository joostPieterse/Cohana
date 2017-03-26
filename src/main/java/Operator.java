
public abstract class Operator {
    protected Chunk chunk;
    protected String action;
    protected Condition condition;
    protected int currentUser;

    protected Tuple getBirthTuple(Tuple firstTuple) {
        Tuple tuple = firstTuple;
        while (tuple.user == currentUser && !action.equals(tuple.action)) {
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
