import org.apache.commons.collections.map.MultiKeyMap;

import java.util.HashMap;

public class AggregationOperator extends Operator {

    long timeStep;
    HashMap<String, Integer> cohortSizeMap = new HashMap();
    MultiKeyMap cohortMetricMap = new MultiKeyMap();

    //timeStep in ms
    public AggregationOperator(Chunk chunk, String action, long timeStep, Condition condition) {
        this.chunk = chunk;
        this.action = action;
        this.timeStep = timeStep;
        this.condition = condition;
    }

    @Override
    public void open() {
        chunk.open();
        AgeSelectionOperator ageSelectionOperator = new AgeSelectionOperator(chunk, action, condition);
        Triple user = chunk.getNextUser();

        //check if the action is done in the chunk at all
        if (!chunk.containsAction(action)) {
            return;
        }
        Tuple tuple = chunk.getNext();
        while (user != null) {
            currentUser = user.u;
            if (tuple == null) {
                break;
            }
            Tuple birthTuple = getBirthTuple(tuple);
            if (birthTuple == null) {
                break;
            }
            //check if user is qualified
            if (condition.isBirthTupleQualified(birthTuple)) {
                //increase cohort size
                Integer currentSize = cohortSizeMap.get(tuple.country);
                if (currentSize == null) {
                    currentSize = 0;
                }
                cohortSizeMap.put(tuple.country, currentSize + 1);
                while (tuple.user == currentUser) {
                    //update metric if qualified
                    if(condition.isAgeTupleQualified(tuple)) {
                        int age = (int) ((tuple.time - birthTuple.time) / timeStep);
                        Integer currentMetric = (Integer) cohortMetricMap.get(tuple.country, age);
                        if (currentMetric == null) {
                            currentMetric = 0;
                        }
                        cohortMetricMap.put(tuple.country, age, currentMetric + tuple.gold);
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
            user = chunk.getNextUser();
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
