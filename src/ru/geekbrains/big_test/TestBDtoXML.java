package ru.geekbrains.big_test;


import org.apache.log4j.*;
import org.apache.log4j.spi.Filter;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.util.*;
/**
 * Created by stan on 02/08/2017.
 * Класс предназначен для работы с базой данных и XML файлами
 * Функционал класса позволяет
 * 1. Записывать базу данных в файл XML
 * 2. Синхронизировать базу данных с заданным файлом  XML
 * 3. Создавать и заполнять базу данных тестовыми данными
 * Логирование осуществляется в файл заданный в соответствующем файле свойств
 */
public class TestBDtoXML {

    private static final Logger log = Logger.getLogger(TestBDtoXML.class);
    private static DataBaseManager dataBaseManager;
    private static Map<NaturalKey, String> data;
    /**
     * указывает путь к файлу свойств
     */
    private static final String PROPERTIES_PATH = "testConfig.properties";
    private static final String SAVE_COMMAND = "save";
    private static final String SYNC_COMMAND = "sync";
    private static final String INIT_COMMAND = "init";

    private static String dbPath;

    /**
     * Точка входа в программу
     * На вход должны подаваться параметры
     * либо save имя_файла
     * либо sync имя_файла
     * либо init
     * @param args - задает параметры
     */
    public static void main(String[] args) {

        loadProperties();

        log.info("setup is done");

        dataBaseManager = new DataBaseManager(dbPath);

        if (args.length < 2) {
            System.out.println("Вы забыли задать параметры для программы. Попробуйте еще раз!");
            System.exit(0);
        }
        switch (args[0]){
            case SAVE_COMMAND:
                File file = new File(args[1]);
                data = dataBaseManager.loadDB();
        try {
            file.createNewFile();
        } catch (IOException e) {
            log.error("something goes wrong with the incoming file", e);
        }
        try {
            writeDataToXML(file);
            System.out.println("Data saved to " + args[1]);
        } catch (ParserConfigurationException | SAXException | IOException e) {
            log.error("Something went wrong with writeDataToXML method",e);
        }
        break;
            case SYNC_COMMAND:
        File sfile = new File(args[1]);
                data = dataBaseManager.loadDB();
        try {
            Map<NaturalKey, String> dataFromFile = parseXML(sfile);
            syncData(dataFromFile);
        } catch (ParserConfigurationException | IOException | SAXException e) {
            log.error("Something went wrong with synchronization",e);
        } catch (DoubleNaturalKeyException e) {
            log.error("XML file contains at least two identical key - synchronixation cancelled",e);
            System.out.println("XML file contains at least two identical key - synchronixation cancelled");
            System.exit(0);
        }
                break;
            case INIT_COMMAND:
                dataBaseManager.initDB();
                System.out.println("new Data created");
                break;
        default:
            System.out.println("Incorrect command");
        }
    }

    /**
     * Осуществляет сравнение данных полученных из базы данных с данными полученными из файла
     * формирует пакеты данных для удаления, добавления и исправления в базе данных
     * передаёт их в качестве параметров в dataBaseManager
     * @see DataBaseManager#updateDB(Set, Map, Map)
     * @param dataFromFile - данные полученные из XML файла с которыми надо синхронизировать базу
     */
    private static void syncData(Map<NaturalKey, String> dataFromFile){
        log.info("syncData started");
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
        log.info("syncData is done");
    }

    /**
     * Считывает данные из XML файла переданного параметром
     * @param syncFile - файл из которого считываются данные
     * @return - возвращает данные содержавшиеся в переданном файле в формате удобном для работы с базой данных
     * @throws ParserConfigurationException - пользовательский класс исключений
     * @throws IOException
     * @throws SAXException
     * @throws DoubleNaturalKeyException
     */
    private static Map<NaturalKey, String> parseXML(File syncFile)
            throws ParserConfigurationException, IOException, SAXException, DoubleNaturalKeyException {
        log.info("parseXML started");
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
        log.info("parseXML is done");
        return result;
    }

    /**
     * Готовит документ для записи его в XML файл и вызывает функцию его записи
     * @see TestBDtoXML#writeDocument(Document, File)
     * @param file - файл в который надо записать данные
     * @throws ParserConfigurationException
     * @throws IOException
     * @throws SAXException
     */
    private static void writeDataToXML(File file) throws ParserConfigurationException, IOException, SAXException {
        log.info("writeDataToXML started");
        DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Document document = documentBuilder.newDocument();
        Element root = document.createElement("DBCopy");
        document.appendChild(root);

        for (Map.Entry<NaturalKey, String> entry: data.entrySet()) {
            addNewEntry(document, entry);
        }
        writeDocument(document, file);
        log.info("writeDataToXML is done");
    }

    /**
     * Добавляет очередную строку данных в документ
     * @see TestBDtoXML#writeDataToXML(File)
     * @param document - документ в который надо добавить данные
     * @param entry - набор данных, который добавляем. Соответствует строке в базе данных.
     */
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

    /**
     * Осуществляет запись подготовленного документа в указанный файл
     * @param document - документ, который надо записать
     * @param file - файл, в который надо записать документ
     */
    private static void writeDocument(Document document, File file)  {
        log.info("writeDocument started");
        try {
            Transformer tr = TransformerFactory.newInstance().newTransformer();
            DOMSource source = new DOMSource(document);
            FileOutputStream fos = new FileOutputStream(file);
            StreamResult result = new StreamResult(fos);
            tr.transform(source, result);
            fos.close();
        } catch (TransformerException | IOException e) {
            log.error("Something went wrong with writing to XML file",e);
        }
        log.info("writeDocument is done");
    }

    /**
     * Загружает параметры из файла свойств
     * @see TestBDtoXML#PROPERTIES_PATH
     * инициализирует путь к базе данных
     * @see TestBDtoXML#dbPath
     * запускает инициализацию логеров для текущего класса и менеджера базы данных
     * @see TestBDtoXML#setupLog(String)
     * @see DataBaseManager#setupLog(String)
     */
    private static void loadProperties(){
        FileInputStream fis;
        Properties properties = new Properties();
        String logPath="";
        try {
            fis = new FileInputStream(PROPERTIES_PATH);
            properties.load(fis);

            dbPath = properties.getProperty("DBpath");
            logPath = properties.getProperty("logFile");

            fis.close();
            File logFile = new File(logPath);
            logFile.createNewFile();
            setupLog(logPath);
            DataBaseManager.setupLog(logPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Инициализирует логер в соответствии с файлом из файла свойств
     * @param logPath - путь к файлу, в который будут писаться логи
     */
    private static void setupLog(String logPath){
        Layout layout = new PatternLayout("%d{ABSOLUTE} %5p %t %C{1}:%M:%L - %m%n");
        Appender fileAppender = null;
        try {
            fileAppender = new FileAppender(layout ,logPath);
            log.addAppender(fileAppender);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
