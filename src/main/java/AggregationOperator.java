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
            //always update cohort size
            Integer currentSize = cohortSizeMap.get(tuple.stringValues.get(cohortColumn));
            if (currentSize == null) {
                currentSize = 0;
            }
            String birthValue = tuple.stringValues.get(cohortColumn);
            cohortSizeMap.put(birthValue, currentSize + 1);
            Tuple birthTuple = getBirthTuple(tuple);
            if (birthTuple == null) {
                tuple = chunk.getNext();
                continue;
            }
            long birthTime = birthTuple.time;
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
                //Workaround for users with only one tuple
                if (chunk.getCurrentUser() == null) {
                    break;
                }
                if (chunk.getCurrentUser().u == currentUser) {
                    chunk.skipCurUser();
                }
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
