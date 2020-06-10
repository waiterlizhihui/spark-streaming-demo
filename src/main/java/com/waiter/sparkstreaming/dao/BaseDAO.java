package com.waiter.sparkstreaming.dao;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.ArrayList;

/**
 * @ClassName BaseDAO
 * @Description TOOD
 * @Author Waiter
 * @Date 2020/5/28 21:39
 * @Version 1.0
 */
public class BaseDAO {
    public static ArrayList selectList(String sql, Object[] queryParms, Class clazz) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet resultSet = null;
        ArrayList resultList = new ArrayList<>();
        try {
            connection = JDBCUtils.getConnection();
            ps = connection.prepareStatement(sql);
            if (queryParms != null) {
                ParameterMetaData pm = ps.getParameterMetaData();
                for (int i = 1; i <= pm.getParameterCount(); i++) {
                    ps.setObject(i, queryParms[i - 1]);
                }
            }
            resultSet = ps.executeQuery();
            while (resultSet.next()) {
                Object obj = mappingObj(resultSet, clazz);
                resultList.add(obj);
            }
            JDBCUtils.free(resultSet, ps, connection);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        return resultList;
    }

    public static Object selectOne(String sql, Object[] queryParms, Class clazz) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet resultSet = null;
        Object result = null;
        try {
            connection = JDBCUtils.getConnection();
            ps = connection.prepareStatement(sql);
            if (queryParms != null) {
                ParameterMetaData pm = ps.getParameterMetaData();
                for (int i = 1; i <= pm.getParameterCount(); i++) {
                    ps.setObject(i, queryParms[i - 1]);
                }
            }
            resultSet = ps.executeQuery();
            while (resultSet.next()) {
                result = mappingObj(resultSet, clazz);
            }
            JDBCUtils.free(resultSet, ps, connection);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static int modifyObj(String sql, Object[] params) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet resultSet = null;
        int modifyResult = 0;
        try {
            connection = JDBCUtils.getConnection();
            ps = connection.prepareStatement(sql);
            if (params != null) {
                ParameterMetaData pm = ps.getParameterMetaData();
                for (int i = 1; i <= pm.getParameterCount(); i++) {
                    ps.setObject(i, params[i - 1]);
                }
            }
            modifyResult = ps.executeUpdate();
            JDBCUtils.free(resultSet, ps, connection);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return modifyResult;
    }

    public static int getTotalRecords(String sql, Object[] params) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet resultSet = null;
        int count = 0;
        try {
            connection = JDBCUtils.getConnection();
            ps = connection.prepareStatement(sql);
            if (params != null) {
                ParameterMetaData pm = ps.getParameterMetaData();
                for (int i = 1; i <= pm.getParameterCount(); i++) {
                    ps.setObject(i, params[i - 1]);
                }
                resultSet = ps.executeQuery();
                while (resultSet.next()) {
                    count = resultSet.getInt(1);
                }
            }
            JDBCUtils.free(resultSet, ps, connection);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return count;
    }

    private static Object mappingObj(ResultSet resultSet, Class clazz) throws IllegalAccessException, InstantiationException, SQLException, InvocationTargetException {
        Object obj = clazz.newInstance();
        Method[] methods = clazz.getMethods();
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i < resultSetMetaData.getColumnCount(); i++) {
            String colName = resultSetMetaData.getColumnLabel(i);
            String methodName = "set" + colName;
            for (Method method : methods) {
                if (method.getName().equalsIgnoreCase(methodName)) {
                    method.invoke(obj, resultSet.getObject(i));
                    break;
                }
            }
        }
        return obj;
    }
}
