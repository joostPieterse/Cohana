
public class BirthSelectionOperator {
    private Chunk chunk;
    private String action;

    private int currentUser;

    public BirthSelectionOperator(Chunk chunk, String action) {
        this.chunk = chunk;
        this.action = action;
    }

    public Tuple getBirthTuple(Tuple firstTuple) {
        Tuple tuple = firstTuple;
        while (tuple.user == currentUser && !action.equals(tuple.action)) {
            tuple = chunk.getNext();
        }
        return tuple;
    }

    public void open() {
        chunk.open();
        currentUser = -1;
    }

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
            if ("bandit".equals(birthTuple.role)) {
                return nextTuple;
            }
            chunk.skipCurUser();
            user = chunk.getNextUser();
            nextTuple = chunk.getNext();
        }
        return null;
    }

}
