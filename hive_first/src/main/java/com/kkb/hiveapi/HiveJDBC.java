package com.kkb.hiveapi;

import java.sql.*;

public class HiveJDBC {
    private static String url="jdbc:hive2://node03:10000/db_hive";
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");

        Connection connection = DriverManager.getConnection(url, "hadoop", "");

        String sql = "select * from score";

        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                String s_id = rs.getString(1);
                String c_id = rs.getString(2);
                String s_score = rs.getString(3);
                System.out.println(s_id + "\t" + c_id + "\t" + s_score);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
