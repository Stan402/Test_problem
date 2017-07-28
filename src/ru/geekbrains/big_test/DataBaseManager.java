package ru.geekbrains.big_test;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DataBaseManager {

    private String path;
    private Connection connection;
    private Statement statement;
    private static final String prepDelete = "DELETE FROM Organogram WHERE DepCode = ? AND DepJob = ?";
    private static final String prepInsert = "INSERT INTO Organogram (DepCode, DepJob, Description) VALUES(?, ?, ?)";
    private static final String prepUpdate = "UPDATE Organogram SET Description = ? WHERE DepCode = ? AND DepJob = ?";

    DataBaseManager(String path){
        this.path = path;
    }

    void updateDB(Set<NaturalKey> toDelete, Map<NaturalKey, String> toAdd, Map<NaturalKey, String> toUpdate){
        connect();
        int a = 0, b = 0, c = 0;
        try {
            connection.setAutoCommit(false);
            Savepoint svpt1 = connection.setSavepoint();

        if (toDelete != null){
            PreparedStatement psDel = connection.prepareStatement(prepDelete);
            for (NaturalKey key: toDelete) {
                psDel.setString(1, key.getDepCode());
                psDel.setString(2, key.getDepJob());
                psDel.addBatch();
            }
            psDel.executeBatch();
            a = toDelete.size();
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
            b = toAdd.size();
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
            c = toUpdate.size();
        }
        connection.setAutoCommit(true);
            System.out.println("deleted: " + a + " added: " + b + " updated: " + c);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        disconnect();
    }

    void initDB() {
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
            e.printStackTrace();
        }
        disconnect();
    }

    Map<NaturalKey, String> loadDB(){
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
            e.printStackTrace();
        }
        disconnect();
        return result;
    }


    public void connect() {
        try {
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection("jdbc:sqlite:" + path);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    public void disconnect() {
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
