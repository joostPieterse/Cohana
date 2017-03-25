import org.apache.commons.collections.keyvalue.MultiKey;
import org.apache.commons.collections.map.MultiKeyMap;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

public class Benchmark {
    Data data = new Data();

    public void test(File file, String delimiter){
        data = new Data();
        System.out.println("Benchmark for "+file.getName());
        long startTime = System.currentTimeMillis();
        data.importData(file, delimiter);
        long duration = System.currentTimeMillis()-startTime;
        System.out.println("Importing data took "+duration+"ms");
        testTablescan();
        testBirthSelection();
        testAgeSelection();
        testAggregation();
    }

    public void testTablescan() {
        long startTime = System.currentTimeMillis();
        int i = 0;
        for (Chunk chunk : data.chunks) {
            Tuple tuple = null;
            chunk.open();
            while ((tuple = chunk.getNext()) != null) {
                //System.out.println(tuple.toString() + " chunk: " + i);
            }
            i++;
        }
        long duration = System.currentTimeMillis()-startTime;
        System.out.println("Table scan done in "+duration+"ms");
    }

    public void testBirthSelection() {
        long startTime = System.currentTimeMillis();
        int i = 0;
        for (Chunk chunk : data.chunks) {
            Tuple tuple = null;
            BirthSelectionOperator op = new BirthSelectionOperator(chunk, "launch");
            op.open();
            while ((tuple = op.getNext()) != null) {
                //System.out.println(tuple.toString() + " chunk: " + i);
            }
            i++;
        }
        long duration = System.currentTimeMillis()-startTime;
        System.out.println("Birth selection done in "+duration+"ms");
    }

    public void testAgeSelection() {
        long startTime = System.currentTimeMillis();
        int i = 0;
        for (Chunk chunk : data.chunks) {
            Tuple tuple = null;
            AgeSelectionOperator op = new AgeSelectionOperator(chunk, "shop");
            op.open();
            while ((tuple = op.getNext()) != null) {
                //System.out.println(tuple.toString() + " chunk: " + i);
            }
            i++;
        }
        long duration = System.currentTimeMillis()-startTime;
        System.out.println("Age selection done in "+duration+"ms");
    }


    public void testAggregation() {
        long startTime = System.currentTimeMillis();
        Data data = new Data();
        data.importData(new File("data.txt"), ";");
        HashMap<String, Integer> cohortSizeMap = new HashMap();
        MultiKeyMap cohortMetricMap = new MultiKeyMap();
        for (Chunk chunk : data.chunks) {
            AggregationOperator op = new AggregationOperator(chunk, "shop", 1000 * 60 * 60 * 24);
            op.open();
            HashMap<String, Integer> chunkCohortSizeMap = op.getCohortSizeMap();
            MultiKeyMap chunkCohortMetricMap = op.getCohortMetricMap();
            String previousCohort = "";
            for (Object o : chunkCohortMetricMap.keySet()) {
                MultiKey multiKey = (MultiKey) o;
                String cohort = (String) multiKey.getKey(0);
                int age = (int) multiKey.getKey(1);
                if (!previousCohort.equals(cohort)) {
                    Integer currentSize = cohortSizeMap.get(cohort);
                    int chunkSize = chunkCohortSizeMap.get(cohort);
                    if (currentSize == null) {
                        currentSize = 0;
                    }
                    cohortSizeMap.put(cohort, currentSize + chunkSize);
                }
                Integer currentMetric = (Integer) cohortMetricMap.get(cohort, age);
                Integer chunkMetric = (Integer) chunkCohortMetricMap.get(cohort, age);
                if (currentMetric == null) {
                    currentMetric = 0;
                }
                cohortMetricMap.put(cohort, age, currentMetric + chunkMetric);
            }
            break;
        }
        long duration = System.currentTimeMillis()-startTime;
        System.out.println("Aggregation done in "+duration+"ms");
        writeToFile(cohortSizeMap, cohortMetricMap, new File("output.txt"));
    }



    private void writeToFile(HashMap<String, Integer> cohortSizeMap, MultiKeyMap cohortMetricMap, File file) {
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(file));
            for (String s : cohortSizeMap.keySet()) {
                bw.write("Cohort: " + s + " size: " + cohortSizeMap.get(s) + "\n");
            }
            bw.write("-----------------------------------------\n");
            for (Object o : cohortMetricMap.keySet()) {
                MultiKey multiKey = (MultiKey) o;
                String cohort = (String) multiKey.getKey(0);
                int age = (int) multiKey.getKey(1);
                bw.write("Cohort: " + cohort + " age: " + age + " metric: " + cohortMetricMap.get(cohort, age) + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (bw != null) {
                    bw.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}