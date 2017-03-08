import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class Data {
    //the index is the id
    public static ArrayList<String> globalActionDict = new ArrayList<>();
    public ArrayList<Chunk> chunks = new ArrayList<>();

    //maximum chunk size in number of lines
    public static final int CHUNK_SIZE = 5;

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
                String action = line[1];
                if (!globalActionDict.contains(action)) {
                    globalActionDict.add(action);
                }
                int actionId = globalActionDict.indexOf(action);
                chunk.insert(Integer.parseInt(user), actionId);
                previousUser = user;
                i++;
            }
            //finalize last chunk
            chunks.get(chunks.size() - 1).finalizeInsert();
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
