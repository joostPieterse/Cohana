import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Chunk {
    private ArrayList<Triple> userColumn = new ArrayList<>();


    private ArrayList<Integer> timeColumnList = new ArrayList<>();

    private Column timeColumn;
    private int minTime;

    private int pointer = 0;
    private int numLines = 0;
    private int userColumnSize = 0;

    //column name to column
    private Map<String, ArrayList<Integer>> columnLists = new HashMap<>();
    private Map<String, Column> columns = new HashMap<>();
    private Map<String, ArrayList<Integer>> dictionaries = new HashMap<>();

    //For integer columns
    private Map<String, Integer> minValues = new HashMap<>();

    //ASSUMES SORTED DATA
    public void insert(int user, int dateMinutes, HashMap<String, Integer> values) {
        numLines++;
        boolean userAlreadyExists = false;
        for (Triple triple : userColumn) {
            if (triple.u == user) {
                userAlreadyExists = true;
                triple.n++;
            }
        }
        if (!userAlreadyExists) {
            userColumn.add(new Triple(user, numLines - 1, 1));
        }
        timeColumnList.add(dateMinutes);

        for (String columnName : Data.columnTypeMap.keySet()) {
            if (!columnLists.containsKey(columnName)) {
                columnLists.put(columnName, new ArrayList<>());
            }
            Type type = Data.columnTypeMap.get(columnName);
            int value = values.get(columnName);
            if (type == Type.INTEGER) {
                columnLists.get(columnName).add(value);
            } else if (type == Type.STRING) {
                if (!dictionaries.containsKey(columnName)) {
                    dictionaries.put(columnName, new ArrayList<>());
                }
                ArrayList<Integer> dict = dictionaries.get(columnName);
                if (!dict.contains(value)) {
                    dict.add(value);
                }
                columnLists.get(columnName).add(value);
            }
        }
    }

    public void finalizeInsert() {
        userColumnSize = userColumn.size();
        for (String columnName : columnLists.keySet()) {
            int min = 0;
            for (int val : columnLists.get(columnName)) {
                min = Math.min(val, min);
            }
            minValues.put(columnName, min);
        }
        for (int val : timeColumnList) {
            minTime = Math.min(val, minTime);
        }
        for (String columnName : columnLists.keySet()) {
            columns.put(columnName, new Column(columnLists.get(columnName)));
        }
        timeColumn = new Column(timeColumnList);
    }

    public void open() {
        pointer = 0;
    }

    public void skipCurUser() {
        Triple currentUser = getCurrentUser();
        pointer = currentUser.f + currentUser.n;
    }

    private Triple getCurrentUser() {
        for (int i = 0; i < userColumn.size(); i++) {
            Triple triple = userColumn.get(i);
            if (triple.f + triple.n > pointer) {
                return triple;
            }
        }
        return null;
    }

    public Triple getNextUser() {
        for (int i = 0; i < userColumnSize; i++) {
            Triple triple = userColumn.get(i);
            if (triple.n > pointer) {
                return userColumn.get(i);
            }
        }
        return null;
    }

    public Tuple getNext() {
        if (pointer >= numLines) {
            return null;
        }
        Triple user = getCurrentUser();
        long timeMinutes = timeColumn.get(pointer) + minTime;
        long timeMillis = timeMinutes * 60 * 1000;

        Map<String, Integer> intValues = new HashMap<>();
        Map<String, String> stringValues = new HashMap<>();
        for (String columnName : columns.keySet()) {
            Column column = columns.get(columnName);
            if (Data.columnTypeMap.get(columnName) == Type.INTEGER) {
                intValues.put(columnName, column.get(pointer) + minValues.get(columnName));
            } else if (Data.columnTypeMap.get(columnName) == Type.STRING) {
                int id = dictionaries.get(columnName).get((int) column.get(pointer));
                stringValues.put(columnName, Data.globalDictionaries.get(columnName).inverse().get(id));
            }
        }
        Tuple tuple = new Tuple(user.u, timeMillis, intValues, stringValues);
        pointer++;
        return tuple;
    }

    public boolean columnContainsString(String columnName, String value) {
        int globalId = Data.binarySearch(value, Data.globalDictionaries.get(columnName));
        if (globalId == -1) {
            return false;
        }
        return dictionaries.get(columnName).contains(globalId);
    }
}
