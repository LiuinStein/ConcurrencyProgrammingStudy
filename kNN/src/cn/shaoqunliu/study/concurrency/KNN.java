package cn.shaoqunliu.study.concurrency;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Predicate;

public class KNN {
    private ArrayList<ArrayList<String>> dataSet;
    private ArrayList<Boolean> labels;
    private ThreadPoolExecutor executor;
    private int k;
    private int threadCount;

    public KNN(String trainSetPath, int k) throws IOException {
        this.k = k;
        Pair<ArrayList<ArrayList<String>>, ArrayList<Boolean>> data = readFile(trainSetPath);
        this.dataSet = data.getKey();
        this.labels = data.getValue();
        threadCount = Runtime.getRuntime().availableProcessors();
        executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadCount);
    }

    private static class Distance implements Comparable<Distance> {
        private Integer index;
        private Double distance;

        public Distance(Integer index, ArrayList<String> a, ArrayList<String> b) {
            this.index = index;
            if (a.size() != b.size()) {
                this.distance = Double.MAX_VALUE;
                return;
            }
            this.distance = 0.0;
            for (int i = 0; i < a.size() - 1; i++) {
                this.distance += Math.pow(sumOfAscii(a.get(i)) - sumOfAscii(b.get(i)), 2);
            }
            this.distance = Math.sqrt(this.distance);
        }

        private long sumOfAscii(String a) {
            long sum = 0;
            for (int i = 0; i < a.length(); i++) {
                sum += a.charAt(i);
            }
            return sum;
        }

        public Integer getIndex() {
            return index;
        }

        public Double getDistance() {
            return distance;
        }

        @Override
        public int compareTo(Distance d) {
            return (distance < d.distance) ? 1 : -1;
        }
    }

    public boolean serialPredict(String test) {
        return serialPredict(readOneLine(test));
    }

    public boolean serialPredict(ArrayList<String> input) {
        ArrayList<Distance> distances = new ArrayList<>(dataSet.size());
        for (int i = 0; i < dataSet.size(); i++) {
            distances.add(new Distance(i, dataSet.get(i), input));
        }
        Collections.sort(distances);
        int tcount = 0, fcount = 0;
        for (int i = 0; i < k && i < distances.size(); i++) {
            if (labels.get(distances.get(i).getIndex())) {
                tcount++;
            } else {
                fcount++;
            }
        }
        return tcount > fcount;
    }

    public boolean parallelPredict(String test) {
        return parallelPredict(readOneLine(test));
    }

    private static class IndividualDistanceTask implements Runnable {

        private List<ArrayList<String>> dataSet;
        private ArrayList<String> input;
        private Distance[] distances;
        private CountDownLatch countDownLatch;
        private Integer index;
        private Integer workLoad;

        public IndividualDistanceTask(List<ArrayList<String>> subSet, ArrayList<String> input, Distance[] distances, CountDownLatch countDownLatch, Integer index, Integer workLoad) {
            this.dataSet = subSet;
            this.input = input;
            this.distances = distances;
            this.countDownLatch = countDownLatch;
            this.index = index;
            this.workLoad = workLoad;
        }

        @Override
        public void run() {
            for (int i = index; i < index + workLoad; i++) {
                distances[i] = new Distance(i, dataSet.get(i), input);
                countDownLatch.countDown();
            }
        }
    }

    public boolean parallelPredict(ArrayList<String> input) {
        Distance[] distances = new Distance[dataSet.size()];
        int taskPerThread = dataSet.size() / threadCount;
        CountDownLatch countDownLatch = new CountDownLatch(dataSet.size());
        for (int i = 0; i < threadCount; i++) {
            IndividualDistanceTask task = new IndividualDistanceTask(
                    dataSet, input, distances, countDownLatch, i * taskPerThread, taskPerThread);
            executor.execute(task);
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Arrays.sort(distances);
        int tcount = 0, fcount = 0;
        for (int i = 0; i < k && i < distances.length; i++) {
            if (labels.get(distances[i].getIndex())) {
                tcount++;
            } else {
                fcount++;
            }
        }
        return tcount > fcount;
    }

    public void destroy() {
        // unreferencing the dataset and labels in order to release memory
        // and preventing this instance from future use after destroying
        // if you use it, a NullPointerException will be thrown
        this.dataSet = null;
        this.labels = null;
        this.executor.shutdown();
        Runtime.getRuntime().gc();
    }

    public double serialTest(String testSetPath) throws IOException {
        return test(testSetPath, this::serialPredict);
    }

    public double parallelTest(String testSetPath) throws IOException {
        return test(testSetPath, this::parallelPredict);
    }

    private double test(String testSetPath, Predicate<ArrayList<String>> predicate) throws IOException {
        Pair<ArrayList<ArrayList<String>>, ArrayList<Boolean>> data = readFile(testSetPath);
        int right = 0;
        for (int i = 0; i < data.getKey().size(); i++) {
            right += (predicate.test(data.getKey().get(i)) == data.getValue().get(i)) ? 1 : 0;
        }
        return (double) right / (double) data.getKey().size();
    }

    private Pair<ArrayList<ArrayList<String>>, ArrayList<Boolean>> readFile(String path) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(path));
        reader.readLine(); // get rid of the first line which represents the notes of every column
        String line = reader.readLine();
        ArrayList<ArrayList<String>> resultDataSet = new ArrayList<>();
        ArrayList<Boolean> resultLabel = new ArrayList<>();
        while (line != null) {
            ArrayList<String> info = readOneLine(line);
            resultLabel.add(info.get(info.size() - 1).equals("yes"));
            info.remove(info.size() - 1);
            resultDataSet.add(info);
            line = reader.readLine();
        }
        return new Pair<>(resultDataSet, resultLabel);
    }

    private ArrayList<String> readOneLine(String line) {
        ArrayList<String> result = new ArrayList<>();
        String[] attributes = line.split(";");
        for (String s : attributes) {
            if (s.charAt(0) == '"') {
                s = s.substring(1, s.length() - 1);
            }
            result.add(s);
        }
        return result;
    }
}
