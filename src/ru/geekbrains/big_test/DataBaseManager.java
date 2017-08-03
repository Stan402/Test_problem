package ru.geekbrains.big_test;

import org.apache.log4j.*;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Класс осуществляет взаимодействие с базой данных для целей задач класса
 * @see TestBDtoXML
 */
class DataBaseManager {

    private static final Logger log = Logger.getLogger(DataBaseManager.class);
    private String path;
    private Connection connection;
    private Statement statement;
    private static final String prepDelete = "DELETE FROM Organogram WHERE DepCode = ? AND DepJob = ?";
    private static final String prepInsert = "INSERT INTO Organogram (DepCode, DepJob, Description) VALUES(?, ?, ?)";
    private static final String prepUpdate = "UPDATE Organogram SET Description = ? WHERE DepCode = ? AND DepJob = ?";

    /**
     * конструктор задает путь к базе данных
     * @param path - путь к базе данных, получен из файла свойств
     *             @see TestBDtoXML#loadProperties()
     */
    DataBaseManager(String path){
        this.path = path;
    }

    /**
     * Осуществляет обновление базы данных в соответствии с переданными параметрами
     * обновление осуществляется одной транзакцией - если где то происходит сбой, данные возвращаются в исходное
     * состояние
     * @param toDelete - блок данных для удаления из базы
     * @param toAdd - блок данных для добавления в базу
     * @param toUpdate - блок данных для коррекции записей в базе
     */
    void updateDB(Set<NaturalKey> toDelete, Map<NaturalKey, String> toAdd, Map<NaturalKey, String> toUpdate){
        log.info("updateDB started");
        connect();
        try {
            Savepoint svpt1 = connection.setSavepoint();
        int countDel = 0, countAdd = 0, countUpdate = 0;
        try {
            connection.setAutoCommit(false);
        if (toDelete != null){
            PreparedStatement psDel = connection.prepareStatement(prepDelete);
            for (NaturalKey key: toDelete) {
                psDel.setString(1, key.getDepCode());
                psDel.setString(2, key.getDepJob());
                psDel.addBatch();
            }
            psDel.executeBatch();
            countDel = toDelete.size();
        }
        if(toAdd != null){
            PreparedStatement psAdd = connection.prepareStatement(prepInsert);
            for (Map.Entry<NaturalKey, String> entry: toAdd.entrySet()) {
                psAdd.setString(1, entry.getKey().getDepCode());
                psAdd.setString(2, entry.getKey().getDepJob());
                psAdd.setString(3, entry.getValue());
                psAdd.addBatch();
            }
            psAdd.executeBatch();
            countAdd = toAdd.size();
        }
        if(toUpdate != null){
            PreparedStatement psUpdate = connection.prepareStatement(prepUpdate);
            for (Map.Entry<NaturalKey, String> entry: toUpdate.entrySet()) {
                psUpdate.setString(1, entry.getValue());
                psUpdate.setString(2, entry.getKey().getDepCode());
                psUpdate.setString(3, entry.getKey().getDepJob());
                psUpdate.addBatch();
            }
            psUpdate.executeBatch();
            countUpdate = toUpdate.size();
        }
        connection.setAutoCommit(true);
            System.out.println("deleted: " + countDel + " added: " + countAdd + " updated: " + countUpdate);
        } catch (SQLException e) {
            connection.rollback();
            log.error("Something went wrong while updating to DataBase", e);
        } finally {
            disconnect();
        }
        } catch (SQLException e) {
            log.error("Something went wrong while creating Savepoint. Transaction cancelled.", e);
            disconnect();
            System.exit(0);
        }
        log.info("updateDB is done");
    }

    /**
     * Метод создает, если требуется, базу данных - если не требуется, очищает старую
     * Заполняет базу тестовыми данными
     */
    void initDB() {
        log.info("initDB is started");
        connect();
        try {
            statement = connection.createStatement();
            statement.execute("CREATE TABLE IF NOT EXISTS Organogram (\n" +
                    "    id     INTEGER PRIMARY KEY AUTOINCREMENT,\n" +
                    "    DepCode TEXT,\n" +
                    "    DepJob  TEXT,\n" +
                    "    Description   TEXT\n" +
                    ");\n");
            statement.execute("DELETE FROM Organogram");
            PreparedStatement psInit = connection.prepareStatement(prepInsert);
            connection.setAutoCommit(false);
            int countTo = 100;
            for (int i = 1; i < countTo; i++) {
                psInit.setString(1, "department" + i/10);
                psInit.setString(2, "worker#" + i);
                psInit.setString(3, "doing some stupid work" + i);
                psInit.addBatch();
            }
            psInit.executeBatch();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            log.error("Something went wrong while initializing the DataBase", e);
        }finally {
            disconnect();
        }
        log.info("initDB is done");
    }

    /**
     * Загружает данные из базы данных в программу
     * @return - возвращает данные базы в формате удобном для обработки основной программой
     */
    Map<NaturalKey, String> loadDB(){
        log.info("loadDB started");
        Map<NaturalKey, String> result = new HashMap<>();
        connect();
        try {
            PreparedStatement psLoadDB = connection.prepareStatement("SELECT * FROM Organogram");
            statement = connection.createStatement();
            ResultSet rs = statement.executeQuery("SELECT * FROM Organogram");
            while (rs.next()){
                result.put(new NaturalKey(rs.getString("DepCode"), rs.getString("DepJob")), rs.getString("Description"));
            }
        } catch (SQLException e) {
            log.error("Something went wrong while loading DataBase", e);
        }
        disconnect();
        log.info("loadDB is done");
        return result;
    }

    /**
     * Открывает доступ к базе данных
     */
    public void connect() {
        try {
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection("jdbc:sqlite:" + path);
        } catch (ClassNotFoundException | SQLException e) {
            log.error("Something went wrong while connecting to DataBase", e);
        }
    }

    /**
     * закрывает доступ к базе данных
     */
    public void disconnect() {
        try {
            connection.close();
        } catch (SQLException e) {
            log.error("Something went wrong while disconnecting from DataBase", e);
        }
    }

    /**
     * Инициализирует логер в соответствии с файлом из файла свойств
     * @see TestBDtoXML#loadProperties()
     * @param logPath - путь к файлу, в который будут писаться логи
     */
    static void setupLog(String logPath){
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
