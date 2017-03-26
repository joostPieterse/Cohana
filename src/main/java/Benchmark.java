import org.apache.commons.collections.keyvalue.MultiKey;
import org.apache.commons.collections.map.MultiKeyMap;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

public class Benchmark {
    Data data = new Data(1000);

    public void test(File file, String delimiter, int chunkSize){
        data = new Data(chunkSize);
        System.out.println("Benchmark for "+file.getName());
        long startTime = System.currentTimeMillis();
        data.importData(file, delimiter);
        long duration = System.currentTimeMillis()-startTime;
        System.out.println("Importing data took "+duration+"ms");
        testTablescan();
        testBirthSelection();
        testAgeSelection();
        query1();
        query3();
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
            BirthSelectionOperator op = new BirthSelectionOperator(chunk, "launch", new Condition() {
                @Override
                public boolean isBirthTupleQualified(Tuple tuple) {
                    return "Australia".equals(tuple.country);
                }

                @Override
                public boolean isAgeTupleQualified(Tuple tuple) {
                    return true;
                }
            });
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
            AgeSelectionOperator op = new AgeSelectionOperator(chunk, "shop", new Condition() {
                @Override
                public boolean isBirthTupleQualified(Tuple tuple) {
                    return true;
                }

                @Override
                public boolean isAgeTupleQualified(Tuple tuple) {
                    return "bandit".equals(tuple.role);
                }
            });
            op.open();
            while ((tuple = op.getNext()) != null) {
                //System.out.println(tuple.toString() + " chunk: " + i);
            }
            i++;
        }
        long duration = System.currentTimeMillis()-startTime;
        System.out.println("Age selection done in "+duration+"ms");
    }


    public void testAggregation(Condition condition, String queryNumber, String birthAction) {
        long startTime = System.currentTimeMillis();
        HashMap<String, Integer> cohortSizeMap = new HashMap();
        MultiKeyMap cohortMetricMap = new MultiKeyMap();
        for (Chunk chunk : data.chunks) {
            AggregationOperator op = new AggregationOperator(chunk, birthAction, 1000 * 60 * 60 * 24, condition);
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
        }
        long duration = System.currentTimeMillis()-startTime;
        System.out.println("Query "+queryNumber+" took "+duration+"ms");
        writeToFile(cohortSizeMap, cohortMetricMap, new File("query"+queryNumber+".txt"));
    }

    private void query1(){
        testAggregation(new Condition() {
            @Override
            public boolean isBirthTupleQualified(Tuple tuple) {
                return true;
            }

            @Override
            public boolean isAgeTupleQualified(Tuple tuple) {
                return true;
            }
        }, "1", "launch");
    }

    private void query3(){
        testAggregation(new Condition() {
            @Override
            public boolean isBirthTupleQualified(Tuple tuple) {
                return true;
            }

            @Override
            public boolean isAgeTupleQualified(Tuple tuple) {
                return "shop".equals(tuple.action);
            }
        }, "3", "shop");
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