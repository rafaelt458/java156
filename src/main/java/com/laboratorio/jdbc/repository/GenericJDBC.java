package com.laboratorio.jdbc.repository;

import com.laboratorio.jdbc.mapper.RowMapper;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class GenericJDBC<T> {
    private final String url;
    private final String user;
    private final String password;

    public GenericJDBC(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }
    
    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(this.url, this.user, this.password);
    }
    
    public long countRecords(String tableName) throws SQLException {
        String query = "SELECT COUNT(*) FROM " + tableName;
        
        Connection connection = this.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(query);
        if (rs.next()) {
            return rs.getLong(1);
        }
        
        return 0L;
    }
    
    public T findById(String tableName, String idColumn, Object idValue, RowMapper<T> rowMapper) throws SQLException {
        String query = "SELECT * FROM " + tableName + " WHERE " + idColumn + " = ?";
        
        Connection connection = this.getConnection();
        PreparedStatement ps = connection.prepareStatement(query);
        ps.setObject(1, idValue);
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            return rowMapper.mapRow(rs);
        }
        
        return null;
    }
    
    public List<T> findAll(String tableName, RowMapper<T> rowMapper) throws SQLException {
        String query = "SELECT * FROM " + tableName;
        List<T> resultList = new ArrayList<>();
        
        Connection connection = this.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(query);
        while (rs.next()) {
            resultList.add(rowMapper.mapRow(rs));
        }
        
        return resultList;
    }
    
    private void setParameters(PreparedStatement ps, Object... params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            ps.setObject(i + 1, params[i]);
        }
    }
    
    public int insertRecord(String query, Object... params) throws SQLException {
        Connection connection = this.getConnection();
        PreparedStatement ps = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
        this.setParameters(ps, params);
        int affectedRows = ps.executeUpdate();
        if (affectedRows > 0) {
            ResultSet rs = ps.getGeneratedKeys();
            if (rs.next()) {
                return rs.getInt(1);
            }
        }
        
        return affectedRows;
    }
    
    public int updateRecord(String query, Object... params) throws SQLException {
        Connection connection = this.getConnection();
        PreparedStatement ps = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
        this.setParameters(ps, params);
        return ps.executeUpdate();
    }
    
    public int deleteRecord(String query, Object... params) throws SQLException {
        return updateRecord(query, params);
    }
}