
public class AggregationOperator extends Operator {
    //HashMap is an array based hash table

    public AggregationOperator(Chunk chunk, String action) {
        this.chunk = chunk;
        this.action = action;
    }

    @Override
    public void open() {
        chunk.open();
        AgeSelectionOperator ageSelectionOperator = new AgeSelectionOperator(chunk, action);
        Triple user = chunk.getNextUser();
        while (user != null) {
            currentUser = user.u;
            Tuple tuple = ageSelectionOperator.getNext();
            Tuple birthTuple = getBirthTuple(tuple);
            //check if user is qualified
            if ("Australia".equals(birthTuple.country)) {
                //increase Hc
                while (tuple.user == currentUser) {
                    
                }
            }
        }
    }

    @Override
    public Tuple getNext() {
        return null;
    }

}
