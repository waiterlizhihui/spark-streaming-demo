package com.waiter.sparkstreaming.dao;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @ClassName JDBCPoolUtils
 * @Description TOOD
 * @Author Waiter
 * @Date 2020/5/28 21:30
 * @Version 1.0
 */
public class JDBCUtils {
    private static DataSource dataSource;

    private JDBCUtils() {
    }

    static {
        Properties prop = new Properties();
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("dbcpconfig.properties");
        try {
            prop.load(inputStream);
            dataSource = DruidDataSourceFactory.createDataSource(prop);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Connection getConnection() {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static void free(ResultSet resultSet, PreparedStatement preparedStatement, Connection connection) {
        try {
            if (null != resultSet) {
                resultSet.close();
            }

            if (null != preparedStatement) {
                preparedStatement.close();
            }

            if (null != connection) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
