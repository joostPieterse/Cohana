
public class AgeSelectionOperator extends Operator{

    public AgeSelectionOperator(Chunk chunk, String action) {
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
        while (nextTuple.user == currentUser) {
            //some condition on the birth tuple
            if ("shop".equals(nextTuple.action) && !"China".equals(nextTuple.country)) {
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
