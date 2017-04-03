import org.apache.commons.collections.map.MultiKeyMap;

import java.util.HashMap;

public class AggregationOperator extends Operator {

    private long timeStep;
    private HashMap<String, Integer> cohortSizeMap = new HashMap();
    private MultiKeyMap cohortMetricMap = new MultiKeyMap();
    private String cohortColumn;
    private String metricColumn;

    //timeStep in ms
    public AggregationOperator(Chunk chunk, String birthValue, String birthColumnName, String cohortColumn, String metricColumn, long timeStep, Condition condition) {
        this.chunk = chunk;
        this.birthValue = birthValue;
        this.birthColumnName = birthColumnName;
        this.cohortColumn = cohortColumn;
        this.metricColumn = metricColumn;
        this.timeStep = timeStep;
        this.condition = condition;
    }

    @Override
    public void open() {
        chunk.open();

        //check if the action is done in the chunk at all
        if (!chunk.columnContainsString(birthColumnName, birthValue)) {
            return;
        }
        Tuple tuple = chunk.getNext();
        while (tuple != null) {
            currentUser = tuple.user;
            Tuple birthTuple = getBirthTuple(tuple);
            if (birthTuple == null) {
                chunk.skipCurUser();
                tuple = chunk.getNext();
                continue;
            }
            //always update cohort size
            Integer currentSize = cohortSizeMap.get(tuple.stringValues.get(cohortColumn));
            if (currentSize == null) {
                currentSize = 0;
            }
            String birthValue = tuple.stringValues.get(cohortColumn);
            long birthTime = birthTuple.time;
            cohortSizeMap.put(birthValue, currentSize + 1);

            //check if user is qualified
            if (condition.isBirthTupleQualified(birthTuple)) {
                while (tuple.user == currentUser) {
                    //update metric if qualified
                    int age = (int) ((tuple.time - birthTime) / timeStep);
                    if (condition.isAgeTupleQualified(tuple)) {
                        Integer currentMetric = (Integer) cohortMetricMap.get(birthValue, age);
                        if (currentMetric == null) {
                            currentMetric = 0;
                        }
                        cohortMetricMap.put(birthValue, age, currentMetric + tuple.intValues.get(metricColumn));
                    }
                    tuple = chunk.getNext();
                    if (tuple == null) {
                        break;
                    }
                }
            } else {
                chunk.skipCurUser();
                tuple = chunk.getNext();
            }
        }
    }

    @Override
    public Object getNext() {
        return null;
    }

    public HashMap<String, Integer> getCohortSizeMap() {
        return cohortSizeMap;
    }

    public MultiKeyMap getCohortMetricMap() {
        return cohortMetricMap;
    }
}
