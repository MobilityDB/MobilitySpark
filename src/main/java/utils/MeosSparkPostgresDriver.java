package utils;

import org.postgresql.Driver;

import java.sql.DriverManager;
import java.sql.SQLException;

public class MeosSparkPosgresDriver extends Driver {
    static{
        try{
            DriverManager.registerDriver(new MeosSparkPosgresDriver());
        } catch (SQLException e){
            throw new RuntimeException("Failed to register custom spark meos driver.", e)
        }
    }
}
