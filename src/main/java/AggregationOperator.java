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
                break;
            }
            //check if user is qualified
            if (condition.isBirthTupleQualified(birthTuple)) {
                //increase cohort size
                Integer currentSize = cohortSizeMap.get(tuple.stringValues.get(cohortColumn));
                if (currentSize == null) {
                    currentSize = 0;
                }
                String birthCountry = tuple.stringValues.get(cohortColumn);
                cohortSizeMap.put(birthCountry, currentSize + 1);

                while (tuple.user == currentUser) {
                    //update metric if qualified
                    if(condition.isAgeTupleQualified(tuple)) {
                        int age = (int) ((tuple.time - birthTuple.time) / timeStep);
                        Integer currentMetric = (Integer) cohortMetricMap.get(birthCountry, age);
                        if (currentMetric == null) {
                            currentMetric = 0;
                        }
                        cohortMetricMap.put(birthCountry, age, currentMetric + tuple.intValues.get(metricColumn));
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
