package ru.geekbrains.big_test;

import org.apache.log4j.*;
import org.apache.log4j.spi.Filter;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Map;

/**
 * Created by stan on 31/07/2017.
 */
public class TestLog {

    private static final Logger log = Logger.getLogger(TestLog.class);

    public static void main(String[] args) {

        String file = "myLog.log";
        Layout layout = new PatternLayout("%d{ABSOLUTE} %5p %t %c{1}:%M:%L - %m%n");
        Filter filter;
        Appender fileAppender = null;
        try {
            fileAppender = new FileAppender(layout ,file);
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.addAppender(fileAppender);
        //log.setLevel(Level.INFO);
        log.info("test before");
        //log.removeAppender("console");
        log.error("test after");
    }



}
