import java.io.File;

public class Main {

    public static void main(String[] args) {
        Data data = new Data();
        data.putData(new File("test.csv"), ",");
        int i = 0;
        for (Chunk chunk : data.chunks) {
            Record record = null;
            chunk.startReading();
            while ((record = chunk.getNext()) != null) {
                System.out.println("User: " + record.user + " action: " + record.action + " chunk: " + i);
            }
            i++;
        }
    }
}
