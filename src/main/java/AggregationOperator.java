import org.apache.commons.collections.map.MultiKeyMap;

import java.util.HashMap;

public class AggregationOperator extends Operator {

    long timeStep;
    HashMap<String, Integer> cohortSizeMap = new HashMap();
    MultiKeyMap cohortMetricMap = new MultiKeyMap();

    //timeStep in ms
    public AggregationOperator(Chunk chunk, String action, long timeStep) {
        this.chunk = chunk;
        this.action = action;
        this.timeStep = timeStep;
    }

    @Override
    public void open() {
        chunk.open();
        AgeSelectionOperator ageSelectionOperator = new AgeSelectionOperator(chunk, action);
        Triple user = chunk.getNextUser();

        //check if the action is done in the chunk at all
        if (!chunk.containsAction(action)) {
            return;
        }
        while (user != null) {
            currentUser = user.u;
            Tuple tuple = ageSelectionOperator.getNext();
            if (tuple == null) {
                break;
            }
            Tuple birthTuple = getBirthTuple(tuple);
            //check if user is qualified
            if ("shop".equals(birthTuple.action)) {
                //increase cohort size
                Integer currentSize = cohortSizeMap.get(tuple.country);
                if (currentSize == null) {
                    currentSize = 0;
                }
                cohortSizeMap.put(tuple.country, currentSize + 1);
                while (tuple.user == currentUser) {
                    //update metric
                    int age = (int) ((tuple.time - birthTuple.time) / timeStep);
                    Integer currentMetric = (Integer) cohortMetricMap.get(tuple.country, age);
                    if (currentMetric == null) {
                        currentMetric = 0;
                    }
                    cohortMetricMap.put(tuple.country, age, currentMetric + tuple.gold);
                    tuple = ageSelectionOperator.getNext();
                    if(tuple==null){
                        break;
                    }
                }
            } else {
                chunk.skipCurUser();
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
