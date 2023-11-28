package com.camel.thread;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class GenerateEmployeeXML {
    private static List<String> generatedNames = new ArrayList<>();

    public static void main(String[] args) {
        Element root = new Element("Employees");
        Document doc = new Document(root);

        generateUniqueNames(1000); // Generate 1000 unique names

        for (int i = 1; i <= 1000; i++) {
            Element employee = generateEmployeeElement(i, getUniqueName(), getRandomDepartmentName());
            root.addContent(employee);
        }

        // Print the absolute path where the file will be created
        String filePath = "output/employees.xml";
        System.out.println("File will be created at: " + new File(filePath).getAbsolutePath());

        writeXmlToFile(doc, filePath);
    }

    private static void generateUniqueNames(int count) {
        List<String> allNames = new ArrayList<>();

        String[] firstNames = {"Chandler", "Ross", "Monica", "Rachel", "Joey", "Phoebe", "Janice", "Gunther", "Emily", "Richard", "Maggie", "Charlie", "Olivia"};
        String[] lastNames = {"Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor", "Anderson", "Harris", "Baker"};

        for (int i = 0; i < count; i++) {
            String fullName = firstNames[i % firstNames.length] + " " + lastNames[i % lastNames.length];
            allNames.add(fullName);
        }

        Collections.shuffle(allNames);
        generatedNames = allNames;
    }

    private static Element generateEmployeeElement(int id, String name, String deptName) {
        Element employee = new Element("employee");
        Element idElement = new Element("id").setText(String.valueOf(id));
        Element nameElement = new Element("name").setText(name);
        Element deptElement = new Element("dept");
        Element deptIdElement = new Element("id").setText(String.valueOf(getRandomDepartmentId()));
        Element deptNameElement = new Element("name").setText(deptName);

        deptElement.addContent(deptIdElement);
        deptElement.addContent(deptNameElement);

        employee.addContent(idElement);
        employee.addContent(nameElement);
        employee.addContent(deptElement);

        return employee;
    }

    private static String getUniqueName() {
        return generatedNames.remove(0);
    }

    private static String getRandomDepartmentName() {
        String[] departmentNames = {"Human Resources", "Marketing", "IT", "Finance", "Operations", "Sales", "Research", "Customer Service", "Legal", "Engineering"};
        Random random = new Random();
        return departmentNames[random.nextInt(departmentNames.length)];
    }

    private static int getRandomDepartmentId() {
        Random random = new Random();
        return random.nextInt(10) + 1; // Random department ID between 1 and 10
    }

    private static void writeXmlToFile(Document document, String fileName) {
        try (FileWriter fileWriter = new FileWriter(fileName)) {
            XMLOutputter xmlOutput = new XMLOutputter();
            xmlOutput.setFormat(Format.getPrettyFormat());
            xmlOutput.output(document, fileWriter);
            System.out.println("File " + fileName + " created successfully!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}