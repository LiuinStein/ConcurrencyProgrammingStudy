package cn.shaoqunliu.study.concurrency;

import java.io.IOException;

public class Main {

    public static void main(String[] args) {
        try {
            KNN knn = new KNN("G:\\Demo\\Java\\ConcurrencyProgrammingStudy\\kNN\\data\\train.csv", 10);
            System.out.println("========== Serial test process started ==========");
            long start = System.currentTimeMillis();
            double accuracy = knn.serialTest("G:\\Demo\\Java\\ConcurrencyProgrammingStudy\\kNN\\data\\test.csv") * 100;
            long end = System.currentTimeMillis();
            System.out.println("Accuracy rate: " + accuracy + "%");
            System.out.println("Time elapsed: " + (end - start) + "ms");
            System.out.println("========== Serial test process ended ==========");
            System.out.println("========== Paralleled test process started ==========");
            start = System.currentTimeMillis();
            accuracy = knn.parallelTest("G:\\Demo\\Java\\ConcurrencyProgrammingStudy\\kNN\\data\\test.csv") * 100;
            end = System.currentTimeMillis();
            System.out.println("Accuracy rate: " + accuracy + "%");
            System.out.println("Time elapsed: " + (end - start) + "ms");
            System.out.println("========== Paralleled test process ended ==========");
            knn.destroy();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
