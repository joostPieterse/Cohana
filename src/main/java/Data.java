import com.google.common.collect.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.*;

public class Data {
    public ArrayList<Chunk> chunks = new ArrayList<>();

    //from column name to type of the column
    public static LinkedHashMap<String, Type> columnTypeMap = new LinkedHashMap<>();

    //the index is the id
    private static Map<String, SortedMap<String, Integer>> unsortedDictionaries = new HashMap<>();
    public static Map<String, ImmutableBiMap<String, Integer>> globalDictionaries = new HashMap<>();

    //maximum chunk size in number of lines
    private int chunkSize = 1000;

    public Data(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public void importData(File file, LinkedHashMap<String, Type> columnTypeMap, String delimiter) {
        Data.columnTypeMap = columnTypeMap;
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

                HashMap<String, Integer> values = new HashMap<>();
                int colNumber = 2;
                for (String columnName : columnTypeMap.keySet()) {
                    String val = line[colNumber];
                    if (columnTypeMap.get(columnName) == Type.INTEGER) {
                        values.put(columnName, Integer.parseInt(val));
                    } else if (columnTypeMap.get(columnName) == Type.STRING) {
                        if (!unsortedDictionaries.containsKey(columnName)) {
                            unsortedDictionaries.put(columnName, new TreeMap<>());
                        }
                        SortedMap<String, Integer> dict = unsortedDictionaries.get(columnName);
                        if (!dict.containsKey(val)) {
                            dict.put(val, dict.size());
                        }
                        values.put(columnName, dict.get(val));
                    }
                    colNumber++;
                }

                chunk.insert(Integer.parseInt(user), dateMinutes, values);
                previousUser = user;
                i++;
            }
            //finalize last chunk
            chunks.get(chunks.size() - 1).finalizeInsert();

            //sort dictionaries
            for (String columnName : unsortedDictionaries.keySet()) {
                globalDictionaries.put(columnName, ImmutableBiMap.copyOf(Maps.newTreeMap(unsortedDictionaries.get(columnName))));
            }
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
        return binarySearch(string, map.keySet().asList(), map.values().asList(), 0, map.size() - 1);
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
