import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

public class Main {
    public static final DateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy/MM/dd:HHmm");

    public static void main(String[] args) {
        Main main = new Main();
        //main.generateData(1000000, "lotsOfData.txt");
        main.testAgeSelection();
    }

    private void test() {
        Data data = new Data();
        data.putData(new File("data.txt"), ";");
        int i = 0;
        for (Chunk chunk : data.chunks) {
            Tuple tuple = null;
            chunk.open();
            while ((tuple = chunk.getNext()) != null) {
                System.out.println(tuple.toString() + " chunk: " + i);
            }
            i++;
        }
    }

    private void testBirthSelection(){
        Data data = new Data();
        data.putData(new File("paperData.txt"), ";");
        int i = 0;
        for (Chunk chunk : data.chunks) {
            Tuple tuple = null;
            BirthSelectionOperator op = new BirthSelectionOperator(chunk, "launch");
            op.open();
            while ((tuple = op.getNext()) != null) {
                System.out.println(tuple.toString() + " chunk: " + i);
            }
            i++;
        }
    }

    private void testAgeSelection(){
        Data data = new Data();
        data.putData(new File("paperData.txt"), ";");
        int i = 0;
        for (Chunk chunk : data.chunks) {
            Tuple tuple = null;
            AggregationOperator op = new AggregationOperator(chunk, "shop");
            op.open();
            while ((tuple = op.getNext()) != null) {
                System.out.println(tuple.toString() + " chunk: " + i);
            }
            i++;
        }
    }

    private void generateData(int numUsers, String fileName) {
        Random r = new Random();
        String[] possibleCountries = {"USA", "China", "Australia", "The Netherlands"};
        String[] possibleRoles = {"dwarf", "assassin", "wizard", "bandit"};
        Calendar cal = Calendar.getInstance(); // creates calendar
        cal.setTime(new Date()); // sets calendar time/date
        Date dateOfPreviousUser = new Date();
        try {
            Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName), "utf-8"));
            for (int i = 0; i < numUsers; i++) {
                cal.setTime(dateOfPreviousUser);
                cal.add(Calendar.HOUR_OF_DAY, r.nextInt(30));
                dateOfPreviousUser = cal.getTime();
                String country = possibleCountries[r.nextInt(possibleCountries.length)];
                //to simulate playing sessions
                int numActionGroups = 1;
                //some users only play once
                if (r.nextBoolean()) {
                    numActionGroups = r.nextInt(40)+1;
                }
                for (int j = 0; j < numActionGroups; j++) {
                    int numActions = r.nextInt(8) + 1;
                    for (int k = 0; k < numActions; k++) {
                        cal.add(Calendar.MINUTE, r.nextInt(30));
                        String action = "launch";
                        String[] otherActions = {"shop", "fight"};
                        if (k != 0) {
                            action = otherActions[r.nextInt(otherActions.length)];
                        }
                        String role = possibleRoles[r.nextInt(possibleRoles.length)];
                        int gold = 0;
                        if ("shop".equals(action)) {
                            gold = (r.nextInt(10) + 1) * 10;
                        }
                        String dateString = DATE_FORMATTER.format(cal.getTime());
                        writer.write(i + ";" + dateString + ";" + action + ";" + role + ";" + country + ";" + gold + "\n");
                    }
                    cal.add(Calendar.DATE, r.nextInt(3));
                }
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
