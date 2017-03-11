import java.util.ArrayList;
import java.util.Date;

public class Chunk {
    private ArrayList<Triple> userColumn = new ArrayList<>();
    private ArrayList<Integer> actionColumnList = new ArrayList<>();
    private ArrayList<Integer> timeColumnList = new ArrayList<>();

    private ArrayList<Integer> chunkActionDict = new ArrayList<>();
    private int minTime, maxTime;

    private Column actionColumn;
    private Column timeColumn;

    private int pointer = 0;
    private int numLines = 0;
    private int userColumnSize = 0;

    //ASSUMES SORTED DATA
    public void insert(int user, int dateMinutes, int actionId) {
        boolean userAlreadyExists = false;
        for (Triple triple : userColumn) {
            if (triple.u == user) {
                userAlreadyExists = true;
                triple.n++;
            }
        }
        if (!userAlreadyExists) {
            userColumn.add(new Triple(user, actionColumnList.size(), 1));
        }
        if (!chunkActionDict.contains(actionId)) {
            chunkActionDict.add(actionId);
        }
        actionColumnList.add(chunkActionDict.indexOf((Integer) actionId));
        timeColumnList.add(dateMinutes);
    }

    public void finalizeInsert() {
        numLines = actionColumnList.size();
        userColumnSize = userColumn.size();
        actionColumn = new Column(actionColumnList);
        for (int time : timeColumnList) {
            minTime = Math.min(time, minTime);
            maxTime = Math.max(time, maxTime);
        }
        timeColumn = new Column(timeColumnList);
    }

    public void startReading() {
        pointer = 0;
    }

    public void skipCurUser() {
        Triple currentUser = getCurrentUser();
        pointer = currentUser.f + currentUser.n;
    }

    //TODO keep track of current user while reading instead?
    private Triple getCurrentUser() {
        for (int i = 0; i < userColumn.size(); i++) {
            Triple triple = userColumn.get(i);
            if (triple.f + triple.n >= pointer) {
                return triple;
            }
        }
        return null;
    }

    public Triple getNextUser() {
        for (int i = 0; i < userColumnSize; i++) {
            Triple triple = userColumn.get(i);
            if (triple.f + triple.n > pointer && i < userColumnSize - 2) {
                return userColumn.get(i + 1);
            }
        }
        return null;
    }

    public Record getNext() {
        if (pointer >= numLines) {
            return null;
        }
        Triple user = getCurrentUser();
        String action = Data.globalActionDict.get(chunkActionDict.get((int) actionColumn.get(pointer)));
        long timeMinutes = getTime(pointer);
        long timeMillis = timeMinutes * 60 * 1000;
        String dateString = Main.DATE_FORMATTER.format(new Date(timeMillis));
        pointer++;
        return new Record(user.u, dateString, action);
    }


    public int getMinTime() {
        return minTime;
    }

    public int getMaxTime() {
        return maxTime;
    }

    private int getTime(int index) {
        return timeColumn.get(index) + minTime;
    }
}
