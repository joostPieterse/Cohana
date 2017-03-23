
public class AgeSelectionOperator {
    private Chunk chunk;
    private String action;

    private int currentUser;

    public AgeSelectionOperator(Chunk chunk, String action) {
        this.chunk = chunk;
        this.action = action;
    }

    public Tuple getBirthTuple(Tuple firstTuple) {
        Tuple tuple = firstTuple;
        while (tuple.user == currentUser && !action.equals(tuple.action)) {
            tuple = chunk.getNext();
            if (tuple == null) {
                return null;
            }
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
        while (nextTuple.user == currentUser) {
            //some condition on the birth tuple
            if ("bandit".equals(nextTuple.role)) {
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
