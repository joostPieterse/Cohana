
public class AggregationOperator extends Operator{

    public AggregationOperator(Chunk chunk, String action) {
        this.chunk = chunk;
        this.action = action;
    }

    @Override
    public void open() {
        chunk.open();
    }

    @Override
    public Tuple getNext() {
        return null;
    }

}
