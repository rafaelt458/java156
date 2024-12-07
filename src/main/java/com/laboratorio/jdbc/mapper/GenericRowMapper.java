package com.laboratorio.jdbc.mapper;

import java.lang.reflect.Field;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;

public class GenericRowMapper<T> implements RowMapper<T> {
    private final Class<T> rowObject;

    public GenericRowMapper(Class<T> rowObject) {
        this.rowObject = rowObject;
    }

    @Override
    public T mapRow(ResultSet rs) throws SQLException {
        try {
            T instance = rowObject.getDeclaredConstructor().newInstance();
            
            Field[] fields = rowObject.getDeclaredFields();
            
            for (Field field : fields) {
                if (field.isAnnotationPresent(ColumnJDBC.class)) {
                    ColumnJDBC column = field.getAnnotation(ColumnJDBC.class);
                    String columnName = column.name();
                    
                    field.setAccessible(true);
                    
                    if (field.getType() == int.class) {
                        field.set(instance, rs.getInt(columnName));
                    } else {
                        if (field.getType() == String.class) {
                            field.set(instance, rs.getString(columnName));
                        } else {
                            if (field.getType() == Date.class) {
                                field.set(instance, rs.getDate(columnName));
                            } else {
                                if (field.getType() == double.class) {
                                    field.set(instance, rs.getDouble(columnName));
                                }
                            }
                        }
                    }
                }
            }
            
            return instance;
        } catch (Exception e) {
            throw new SQLException("Error mapeando el registro en el objeto", e);
        }
    }
}