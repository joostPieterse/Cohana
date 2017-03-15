import com.google.common.collect.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Data {
    //the index is the id
    private static SortedMap<String, Integer> unsortedGlobalActionDict = new TreeMap<>();
    public static ImmutableBiMap<String, Integer> globalActionDict;
    public ArrayList<Chunk> chunks = new ArrayList<>();


    private int minTime, maxTime;

    //maximum chunk size in number of lines
    public static final int CHUNK_SIZE = 1000;

    public void putData(File file, String delimiter) {
        int chunkNumber = 0;
        int i = 0;
        String previousUser = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String lineString;
            while ((lineString = br.readLine()) != null) {
                String[] line = lineString.split(delimiter);
                String user = line[0];
                if (i > CHUNK_SIZE && !user.equals(previousUser)) {
                    chunks.get(chunkNumber).finalizeInsert();
                    i = 0;
                    chunkNumber++;
                }
                if (chunkNumber >= chunks.size()) {
                    chunks.add(new Chunk());
                }
                Chunk chunk = chunks.get(chunkNumber);
                String dateString = line[1];
                Date date = Main.DATE_FORMATTER.parse(dateString);
                long dateMillis = date.getTime();
                int dateMinutes = (int) (dateMillis / 1000 / 60);
                String action = line[2];
                if (!unsortedGlobalActionDict.containsKey(action)) {
                    unsortedGlobalActionDict.put(action, unsortedGlobalActionDict.size());
                }
                int actionId = unsortedGlobalActionDict.get(action);
                chunk.insert(Integer.parseInt(user), dateMinutes, actionId);
                previousUser = user;
                i++;
            }
            //finalize last chunk
            chunks.get(chunks.size() - 1).finalizeInsert();

            //sort dictionaries
            globalActionDict = ImmutableBiMap.copyOf(Maps.newTreeMap(unsortedGlobalActionDict));

            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    //Binary search for finding a global id in a disctionary for a string column
    //gives -1 if not found
    public int binarySearch(String string, ImmutableBiMap<String, Integer> map) {
        return binarySearch(string, map, map.keySet().asList(), map.values().asList(), 0, map.size() - 1);
    }

    private int binarySearch(String string, ImmutableBiMap<String, Integer> map, ImmutableList<String> keyList, ImmutableList<Integer> valueList, int low, int high) {
        if (low > high) {
            return -1;
        }
        int mid = (low + high) / 2;
        String midString = keyList.get(mid);
        if (string.compareTo(midString) < 0) {
            return binarySearch(string, map, keyList, valueList, low, mid - 1);
        } else if (string.compareTo(midString) > 0) {
            return binarySearch(string, map, keyList, valueList, mid + 1, high);
        } else {
            return valueList.get((int) mid);
        }

    }

    public int getMinTime() {
        return minTime;
    }

    public int getMaxTime() {
        return maxTime;
    }
}
