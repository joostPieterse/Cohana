import java.util.ArrayList;
import java.util.Date;

public class Chunk {
    private ArrayList<Triple> userColumn = new ArrayList<>();

    private ArrayList<Integer> actionColumnList = new ArrayList<>();
    private ArrayList<Integer> roleColumnList = new ArrayList<>();
    private ArrayList<Integer> countryColumnList = new ArrayList<>();

    private ArrayList<Integer> timeColumnList = new ArrayList<>();
    private ArrayList<Integer> goldColumnList = new ArrayList<>();

    private ArrayList<Integer> chunkActionDict = new ArrayList<>();
    private ArrayList<Integer> chunkRoleDict = new ArrayList<>();
    private ArrayList<Integer> chunkCountryDict = new ArrayList<>();
    private int minTime, maxTime;
    private int minGold;


    private int maxGold;

    private Column actionColumn;
    private Column countryColumn;
    private Column roleColumn;
    private Column timeColumn;
    private Column goldColumn;

    private int pointer = 0;
    private int numLines = 0;
    private int userColumnSize = 0;

    //ASSUMES SORTED DATA
    public void insert(int user, int dateMinutes, int actionId, int roleId, int countryId, int gold) {
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
        if (!chunkRoleDict.contains(roleId)) {
            chunkRoleDict.add(roleId);
        }
        roleColumnList.add(chunkRoleDict.indexOf((Integer) roleId));
        if (!chunkCountryDict.contains(countryId)) {
            chunkCountryDict.add(countryId);
        }
        countryColumnList.add(chunkCountryDict.indexOf((Integer) countryId));

        timeColumnList.add(dateMinutes);
        goldColumnList.add(gold);
    }

    public void finalizeInsert() {
        numLines = actionColumnList.size();
        userColumnSize = userColumn.size();
        actionColumn = new Column(actionColumnList);
        roleColumn = new Column(roleColumnList);
        countryColumn = new Column(countryColumnList);
        for (int time : timeColumnList) {
            minTime = Math.min(time, minTime);
            maxTime = Math.max(time, maxTime);
        }
        timeColumn = new Column(timeColumnList);
        for (int gold : goldColumnList) {
            minGold = Math.min(gold, minTime);
            maxGold = Math.max(gold, maxTime);
        }
        goldColumn = new Column(goldColumnList);
    }

    public void open() {
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
            if (triple.f + triple.n > pointer - 1) {
                return triple;
            }
        }
        return null;
    }

    public Triple getNextUser() {
        for (int i = 0; i < userColumnSize; i++) {
            Triple triple = userColumn.get(i);
            if (triple.f + triple.n > pointer) {
                return userColumn.get(i);
            }
        }
        return null;
    }

    public Tuple getNext() {
        if (pointer >= numLines) {
            return null;
        }
        Triple user = getNextUser();
        String action = Data.globalActionDict.inverse().get(chunkActionDict.get((int) actionColumn.get(pointer)));
        String role = Data.globalRoleDict.inverse().get(chunkRoleDict.get((int) roleColumn.get(pointer)));
        String country = Data.globalCountryDict.inverse().get(chunkCountryDict.get((int) countryColumn.get(pointer)));
        long timeMinutes = getTime(pointer);
        long timeMillis = timeMinutes * 60 * 1000;
        int gold = getGold(pointer);
        pointer++;
        return new Tuple(user.u, action, timeMillis, role, country, gold);
    }


    public int getMinTime() {
        return minTime;
    }

    public int getMaxTime() {
        return maxTime;
    }

    public int getMinGold() {
        return minGold;
    }

    public int getMaxGold() {
        return maxGold;
    }

    private int getTime(int index) {
        return timeColumn.get(index) + minTime;
    }

    private int getGold(int index) {
        return goldColumn.get(index) + minGold;
    }
}
