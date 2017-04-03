import java.util.HashMap;
import java.util.Map;

public class Tuple {
    int user;
    long time;
    Map<String, Integer> intValues = new HashMap<>();
    Map<String, String> stringValues = new HashMap<>();

    public Tuple(int user, long time, Map<String, Integer> intValues, Map<String, String> stringValues) {
        this.user = user;
        this.time = time;
        this.intValues = new HashMap<>(intValues);
        this.stringValues = new HashMap<>(stringValues);
    }

    @Override
    public String toString() {
        String result = "{user:" + user + " time:" + time;
        for(String columnName:intValues.keySet()){
            result += " "+columnName+":"+intValues.get(columnName);
        }
        for(String columnName:stringValues.keySet()){
            result += " "+columnName+":"+stringValues.get(columnName);
        }
        result += "}";
        return result;
    }
}
