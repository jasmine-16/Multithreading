package com.camel.thread;

//import java.io.*;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.atomic.AtomicInteger;
//import javax.xml.parsers.DocumentBuilder;
//import javax.xml.parsers.DocumentBuilderFactory;
//import javax.xml.xpath.*;
//import org.w3c.dom.Document;
//import org.w3c.dom.NodeList;
//import org.xml.sax.InputSource;
//
//public class ProcessDataWithThreads {
//    public static void main(String[] args) {
//        try {
//            File inputFile = new File("output/employees.xml");
//            BufferedReader reader = new BufferedReader(new FileReader(inputFile));
//            BufferedWriter writer = new BufferedWriter(new FileWriter("output/output.csv"));
//
//            // Write the CSV header
//            writer.write("id,name,dept_id,dept_name");
//            writer.newLine();
//
//            ExecutorService executorService = Executors.newFixedThreadPool(5);
//
//            System.out.println("Processing records...");
//
//            Document document = parseXmlFile(inputFile);
//
//            XPathFactory xPathFactory = XPathFactory.newInstance();
//            XPath xPath = xPathFactory.newXPath();
//
//            XPathExpression expr = xPath.compile("//employee");
//            NodeList nodeList = (NodeList) expr.evaluate(document, XPathConstants.NODESET);
//
//            AtomicInteger recordCount = new AtomicInteger(0);
//            int batchSize = 2000; // Adjust the batch size as needed
//
//            for (int i = 0; i < nodeList.getLength(); i += batchSize) {
//                final int startIdx = i;
//                final int endIdx = Math.min(i + batchSize, nodeList.getLength());
//
//                executorService.execute(() -> {
//                    processRecordsInRange(nodeList, startIdx, endIdx, writer, recordCount);
//                });
//            }
//
//            executorService.shutdown();
//            while (!executorService.isTerminated()) {
//            }
//
//            writer.close();
//            reader.close();
//            System.out.println("All records have been processed.");
//        } catch (IOException | XPathExpressionException e) {
//            e.printStackTrace();
//        }
//    }
//
//    private static Document parseXmlFile(File file) {
//        try {
//            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
//            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
//            return dBuilder.parse(new InputSource(new FileReader(file)));
//        } catch (Exception e) {
//            e.printStackTrace();
//            return null;
//        }
//    }
//
//    private static void processRecordsInRange(NodeList nodeList, int startIdx, int endIdx, BufferedWriter writer, AtomicInteger recordCount) {
//        for (int i = startIdx; i < endIdx; i++) {
//            processRecord(nodeList.item(i), writer, recordCount);
//        }
//    }
//
//    private static void processRecord(org.w3c.dom.Node node, BufferedWriter writer, AtomicInteger recordCount) {
//        System.out.println("Processing record...");
//
//        String id = evaluateXPath(node, "id");
//        String name = evaluateXPath(node, "name");
//        String deptId = evaluateXPath(node, "dept/id");
//        String deptName = evaluateXPath(node, "dept/name");
//
//        try {
//            synchronized (writer) {
//                writer.write(id + "," + name + "," + deptId + "," + deptName);
//                writer.newLine();
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    private static String evaluateXPath(org.w3c.dom.Node node, String expression) {
//        try {
//            XPathFactory xPathFactory = XPathFactory.newInstance();
//            XPath xPath = xPathFactory.newXPath();
//            XPathExpression expr = xPath.compile(expression);
//            return expr.evaluate(node);
//        } catch (XPathExpressionException e) {
//            e.printStackTrace();
//            return "";
//        }
//    }
//}

//
//import java.io.*;
//import java.text.SimpleDateFormat;
//import java.util.Date;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.ScheduledExecutorService;
//import java.util.concurrent.atomic.AtomicInteger;
//import javax.xml.parsers.DocumentBuilder;
//import javax.xml.parsers.DocumentBuilderFactory;
//import javax.xml.xpath.*;
//import org.w3c.dom.Document;
//import org.w3c.dom.NodeList;
//import org.xml.sax.InputSource;
//
//public class ProcessDataWithThreads {
//    public static void main(String[] args) {
//        try {
//            File inputFile = new File("output/employees.xml");
//            BufferedReader reader = new BufferedReader(new FileReader(inputFile));
//            BufferedWriter writer = new BufferedWriter(new FileWriter("output/output.csv"));
//
//            // Write the CSV header
//            writer.write("id,name,dept_id,dept_name");
//            writer.newLine();
//
////            ExecutorService executorService = Executors.newFixedThreadPool(5);
//            ScheduledExecutorService executorService = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
//
//            System.out.println("Processing records...");
//
//            Document document = parseXmlFile(inputFile);
//
//            XPathFactory xPathFactory = XPathFactory.newInstance();
//            XPath xPath = xPathFactory.newXPath();
//
//            XPathExpression expr = xPath.compile("//employee");
//            NodeList nodeList = (NodeList) expr.evaluate(document, XPathConstants.NODESET);
//
//            AtomicInteger recordCount = new AtomicInteger(0);
//            int batchSize = 200; // Adjust the batch size as needed
//
//            for (int i = 0; i < nodeList.getLength(); i += batchSize) {
//                final int startIdx = i;
//                final int endIdx = Math.min(i + batchSize, nodeList.getLength());
//
//                executorService.execute(() -> {
//                    processRecords(nodeList, startIdx, endIdx, writer, recordCount);
//                });
//
//                System.out.println("Submitted batch " + (i / batchSize + 1));
//            }
//
//            executorService.shutdown();
//            while (!executorService.isTerminated()) {
//            }
//
//            writer.close();
//            reader.close();
//            System.out.println("All records have been processed.");
//        } catch (IOException | XPathExpressionException e) {
//            e.printStackTrace();
//        }
//    }
//
//    private static Document parseXmlFile(File file) {
//        try {
//            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
//            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
//            return dBuilder.parse(new InputSource(new FileReader(file)));
//        } catch (Exception e) {
//            e.printStackTrace();
//            return null;
//        }
//    }
//
//    private static void processRecords(NodeList nodeList, int startIdx, int endIdx, BufferedWriter writer, AtomicInteger recordCount) {
//        for (int i = startIdx; i < endIdx; i++) {
//            processRecord(nodeList.item(i), writer, recordCount);
//        }
//    }
//
//
//    private static void processRecord(org.w3c.dom.Node node, BufferedWriter writer, AtomicInteger recordCount) {
//        String threadsinfo="Thread "+ Thread.currentThread().getName();
//        String timestampStart = getCurrentTimestamp();
//
//        System.out.println(threadsinfo+"  Processing record..." + timestampStart);
//
//
//        String id = evaluateXPath(node, "id");
//        String name = evaluateXPath(node, "name");
//        String deptId = evaluateXPath(node, "dept/id");
//        String deptName = evaluateXPath(node, "dept/name");
//        String timestampEnd = getCurrentTimestamp();
//
//        try {
//            synchronized (writer) {
//                writer.write(id + "," + name + "," + deptId + "," + deptName);
//                writer.newLine();
//                recordCount.incrementAndGet();
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        System.out.println(threadsinfo + "  Processing record... End Time: " + timestampEnd);
//    }
//
//    private static String getCurrentTimestamp() {
//        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//        return dateFormat.format(new Date());
//
//    }
//
//    private static String evaluateXPath(org.w3c.dom.Node node, String expression) {
//        try {
//            XPathFactory xPathFactory = XPathFactory.newInstance();
//            XPath xPath = xPathFactory.newXPath();
//            XPathExpression expr = xPath.compile(expression);
//            return expr.evaluate(node);
//        } catch (XPathExpressionException e) {
//            e.printStackTrace();
//            return "";
//        }
//    }
//}














import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.*;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class ProcessDataWithThreads {
    private static final AtomicInteger recordCount = new AtomicInteger(0);

    public static void main(String[] args) {
        try {
            File inputFile = new File("output/employees.xml");
            BufferedReader reader = new BufferedReader(new FileReader(inputFile));
            BufferedWriter writer = new BufferedWriter(new FileWriter("output/output.csv"));

            // Write the CSV header
            writer.write("id,name,dept_id,dept_name");
            writer.newLine();

//            ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
            ScheduledExecutorService executorService = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
            System.out.println("Processing records...");

            Document document = parseXmlFile(inputFile);

            XPathFactory xPathFactory = XPathFactory.newInstance();
            XPath xPath = xPathFactory.newXPath();

            XPathExpression expr = xPath.compile("//employee");
            NodeList nodeList = (NodeList) expr.evaluate(document, XPathConstants.NODESET);

            int batchSize = 200;

            List<CompletableFuture<Void>> futures = new ArrayList<>();

            for (int i = 0; i < nodeList.getLength(); i += batchSize) {
                final int startIdx = i;
                final int endIdx = Math.min(i + batchSize, nodeList.getLength());

                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    processRecordsAsync(nodeList, startIdx, endIdx, writer);
                }, executorService);

                futures.add(future);
                System.out.println("Submitted batch " + (i / batchSize + 1));
            }

            // Wait for all CompletableFuture to complete
            CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            allOf.join();

            executorService.shutdown();

            writer.close();
            reader.close();
            System.out.println("All records have been processed.");
        } catch (IOException | XPathExpressionException e) {
            e.printStackTrace();
        }
    }

    private static Document parseXmlFile(File file) {
        try {
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            return dBuilder.parse(new InputSource(new FileReader(file)));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static void processRecordsAsync(NodeList nodeList, int startIdx, int endIdx, BufferedWriter writer) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int i = startIdx; i < endIdx; i++) {
            final int currentIndex = i;  // Create a final local variable
            CompletableFuture<Void> recordFuture = CompletableFuture.supplyAsync(() -> {
                return extractFields(nodeList.item(currentIndex));
            }).thenAcceptAsync(fields -> {
                writeRecord(fields, writer);
            });

            futures.add(recordFuture);
        }

        CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        allOf.join();
    }

    private static String[] extractFields(org.w3c.dom.Node node) {
        String id = evaluateXPath(node, "id");
        String name = evaluateXPath(node, "name");
        String deptId = evaluateXPath(node, "dept/id");
        String deptName = evaluateXPath(node, "dept/name");
        return new String[]{id, name, deptId, deptName};
    }

    private static void writeRecord(String[] fields, BufferedWriter writer) {
        String threadsInfo = "Thread " + Thread.currentThread().getName();
        String timestampStart = getCurrentTimestamp();

        System.out.println(threadsInfo + " Start Processing record..." + timestampStart);

        try {
            synchronized (writer) {
                writer.write(String.join(",", fields));
                writer.newLine();
                recordCount.incrementAndGet();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        String timestampEnd = getCurrentTimestamp();
        System.out.println(threadsInfo + "  Processing record... End Time: " + timestampEnd);
    }

    private static String getCurrentTimestamp() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return dateFormat.format(new Date());
    }

    private static String evaluateXPath(org.w3c.dom.Node node, String expression) {
        try {
            XPathFactory xPathFactory = XPathFactory.newInstance();
            XPath xPath = xPathFactory.newXPath();
            XPathExpression expr = xPath.compile(expression);
            return expr.evaluate(node);
        } catch (XPathExpressionException e) {
            e.printStackTrace();
            return "";
        }
    }
}
