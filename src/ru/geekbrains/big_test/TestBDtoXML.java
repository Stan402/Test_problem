package ru.geekbrains.big_test;


import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import ru.geekbrains.big_test.DataBaseManager;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.util.*;

public class TestBDtoXML {

    private static DataBaseManager dataBaseManager;
    private static Map<NaturalKey, String> data;

    private static final String PROPERTIES_PATH = "testConfig.properties";
    private static final String SAVE_COMMAND = "save";
    private static final String SYNC_COMMAND = "sync";

    private static String dbPath;
    private static String logPath;


    public static void main(String[] args) {

        loadProperties();

        dataBaseManager = new DataBaseManager(dbPath);
        data = dataBaseManager.loadDB();

        if (args.length < 2) {
            System.out.println("Вы забыли задать параметры для программы. Попробуйте еще раз!");
            System.exit(0);
        }
        switch (args[0]){
            case SAVE_COMMAND:
                File file = new File(args[1]);
        try {
            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            writeDataToXML(file);
        } catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
        }
        break;
            case SYNC_COMMAND:
        File sfile = new File(args[1]);
        try {
            Map<NaturalKey, String> dataFromFile = parceXML(sfile);
            syncData(dataFromFile);
        } catch (ParserConfigurationException | IOException | SAXException e) {
            e.printStackTrace();
        }
        break;
        default:
            System.out.println("Передана некорректная команда");
        }
//        for (Map.Entry<NaturalKey, String> entry: data.entrySet()) {
//            System.out.println(entry.getKey()+" " + entry.getValue());
//        }

//        File sfile = new File(syncFile);
//        try {
//            Map<NaturalKey, String> dataFromFile = parceXML(sfile);
//            syncData(dataFromFile);
//        } catch (ParserConfigurationException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (SAXException e) {
//            e.printStackTrace();
//        }


        //testBDtoXML.dataBaseManager.initDB();
    }

    private static void syncData(Map<NaturalKey, String> dataFromFile){
         Map<NaturalKey, String> dataToAdd = new HashMap<>();
         Map<NaturalKey, String> dataToUpdate = new HashMap<>();
         Set<NaturalKey> dataToDelete = new HashSet<>();

        for (Map.Entry<NaturalKey, String> entry:data.entrySet()) {
            if (!dataFromFile.containsKey(entry.getKey())) dataToDelete.add(entry.getKey());
        }
        for (Map.Entry<NaturalKey, String> entry:dataFromFile.entrySet()) {
            if (!data.containsKey(entry.getKey())) dataToAdd.put(entry.getKey(), entry.getValue());
            else if (!entry.getValue().equals(data.get(entry.getKey()))) dataToUpdate.put(entry.getKey(), entry.getValue());
        }

        dataBaseManager.updateDB(dataToDelete, dataToAdd, dataToUpdate);

    }

    private static Map<NaturalKey, String> parceXML(File syncFile) throws ParserConfigurationException, IOException, SAXException {
        Map<NaturalKey, String> result = new HashMap<>();
        DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Document document = documentBuilder.parse(syncFile);
        Node root = document.getDocumentElement();
        NodeList lines = root.getChildNodes();
        for (int i = 0; i < lines.getLength(); i++) {
            Node line = lines.item(i);
            NodeList fields = line.getChildNodes();
            String depCode = fields.item(0).getTextContent();
            String depJob = fields.item(1).getTextContent();
            String descrptn = fields.item(2).getTextContent();
            if(depCode.length() > 20 || depJob.length() > 100 || descrptn.length() > 255){
                System.out.println("wrong field size in " + i + " line");
                return null;
            }
            NaturalKey key = new NaturalKey(depCode, depJob);
            if (result.containsKey(key)) throw new DoubleNaturalKeyException();
            result.put(key, descrptn);
        }
        return result;
    }

    private static void writeDataToXML(File file) throws ParserConfigurationException, IOException, SAXException {
        DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Document document = documentBuilder.newDocument();
        Element root = document.createElement("DBCopy");
        document.appendChild(root);

        for (Map.Entry<NaturalKey, String> entry: data.entrySet()) {
            addNewEntry(document, entry);
        }
        writeDocument(document, file);
    }

    private static void addNewEntry(Document document, Map.Entry<NaturalKey, String> entry){
        Node root = document.getDocumentElement();
        Element line = document.createElement("Line");
        Element depCode = document.createElement("DepCode");
        depCode.setTextContent(entry.getKey().getDepCode());
        Element depJob = document.createElement("DepJob");
        depJob.setTextContent(entry.getKey().getDepJob());
        Element description = document.createElement("Description");
        description.setTextContent(entry.getValue());
        line.appendChild(depCode);
        line.appendChild(depJob);
        line.appendChild(description);
        root.appendChild(line);
    }

    private static void writeDocument(Document document, File file)  {
        try {
            Transformer tr = TransformerFactory.newInstance().newTransformer();
            DOMSource source = new DOMSource(document);
            FileOutputStream fos = new FileOutputStream(file);
            StreamResult result = new StreamResult(fos);
            tr.transform(source, result);
            fos.close();
        } catch (TransformerException | IOException e) {
            e.printStackTrace();
        }


    }

    private static void loadProperties(){
        FileInputStream fis;
        Properties properties = new Properties();

        try {
            fis = new FileInputStream(PROPERTIES_PATH);
            properties.load(fis);

            dbPath = properties.getProperty("DBpath");
            logPath = properties.getProperty("logFile");

            fis.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
