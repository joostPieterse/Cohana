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
    private static SortedMap<String, Integer> unsortedGlobalRoleDict = new TreeMap<>();
    public static ImmutableBiMap<String, Integer> globalRoleDict;
    private static SortedMap<String, Integer> unsortedGlobalCountryDict = new TreeMap<>();
    public static ImmutableBiMap<String, Integer> globalCountryDict;
    public ArrayList<Chunk> chunks = new ArrayList<>();


    private int minTime, maxTime;

    //maximum chunk size in number of lines
    private  int chunkSize = 1000;

    public Data(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public void importData(File file, String delimiter) {
        int chunkNumber = 0;
        int i = 0;
        String previousUser = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String lineString;
            while ((lineString = br.readLine()) != null) {
                String[] line = lineString.split(delimiter);
                String user = line[0];
                if (i > chunkSize && !user.equals(previousUser)) {
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
                int gold = Integer.parseInt(line[5]);
                String action = line[2];
                if (!unsortedGlobalActionDict.containsKey(action)) {
                    unsortedGlobalActionDict.put(action, unsortedGlobalActionDict.size());
                }
                int actionId = unsortedGlobalActionDict.get(action);
                String role = line[3];
                if (!unsortedGlobalRoleDict.containsKey(role)) {
                    unsortedGlobalRoleDict.put(role, unsortedGlobalRoleDict.size());
                }
                int roleId = unsortedGlobalRoleDict.get(role);
                String country = line[4];
                if (!unsortedGlobalCountryDict.containsKey(country)) {
                    unsortedGlobalCountryDict.put(country, unsortedGlobalCountryDict.size());
                }
                int countryId = unsortedGlobalCountryDict.get(country);
                chunk.insert(Integer.parseInt(user), dateMinutes, actionId, roleId, countryId, gold);
                previousUser = user;
                i++;
            }
            //finalize last chunk
            chunks.get(chunks.size() - 1).finalizeInsert();

            //sort dictionaries
            globalActionDict = ImmutableBiMap.copyOf(Maps.newTreeMap(unsortedGlobalActionDict));
            globalRoleDict = ImmutableBiMap.copyOf(Maps.newTreeMap(unsortedGlobalRoleDict));
            globalCountryDict = ImmutableBiMap.copyOf(Maps.newTreeMap(unsortedGlobalCountryDict));

            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    //Binary search for finding a global id in a dictionary for a string column
    //gives -1 if not found
    public static int binarySearch(String string, ImmutableBiMap<String, Integer> map) {
        return binarySearch(string,map.keySet().asList(), map.values().asList(), 0, map.size() - 1);
    }

    private static int binarySearch(String string, ImmutableList<String> keyList, ImmutableList<Integer> valueList, int low, int high) {
        if (low > high) {
            return -1;
        }
        int mid = (low + high) / 2;
        String midString = keyList.get(mid);
        if (string.compareTo(midString) < 0) {
            return binarySearch(string, keyList, valueList, low, mid - 1);
        } else if (string.compareTo(midString) > 0) {
            return binarySearch(string, keyList, valueList, mid + 1, high);
        } else {
            return valueList.get((int) mid);
        }

    }
}
