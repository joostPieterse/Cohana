
public class BirthSelectionOperator extends Operator{

    public BirthSelectionOperator(Chunk chunk, String action) {
        this.chunk = chunk;
        this.action = action;
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
            if ("Australia".equals(birthTuple.country)) {
                return nextTuple;
            }
            chunk.skipCurUser();
            user = chunk.getNextUser();
            nextTuple = chunk.getNext();
        }
        return null;
    }

}
