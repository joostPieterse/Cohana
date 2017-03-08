import java.util.ArrayList;

public class Chunk {
    private ArrayList<Triple> userColumn = new ArrayList<>();
    private ArrayList<Integer> actionColumnList = new ArrayList<>();
    private ArrayList<Integer> timeColumn = new ArrayList<>();

    private ArrayList<Integer> chunkActionDict = new ArrayList<>();

    private Column actionColumn;

    private int pointer = 0;
    private int numLines = 0;
    private int userColumnSize = 0;

    //ASSUMES SORTED DATA
    public void insert(int user, int actionId) {
        boolean userAlreadyExists = false;
        for (Triple triple : userColumn) {
            if (triple.u == user) {
                userAlreadyExists = true;
                triple.n++;
            }
        }
        if (!userAlreadyExists) {
            userColumn.add(new Triple(user, userColumn.size(), 1));
        }
        if (!chunkActionDict.contains(actionId)) {
            chunkActionDict.add(actionId);
        }
        actionColumnList.add(chunkActionDict.indexOf((Integer) actionId));
    }

    public void finalizeInsert() {
        numLines = actionColumnList.size();
        userColumnSize = userColumn.size();
        actionColumn = new Column(actionColumnList);
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
        //TODO time

        pointer++;
        return new Record(user.u, action, 0);
    }
}
