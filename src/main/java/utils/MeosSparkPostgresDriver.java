package utils;

import org.postgresql.Driver;
import org.postgresql.PGConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class MeosSparkPostgresDriver extends Driver {
    static{
        try{
            DriverManager.registerDriver(new MeosSparkPostgresDriver());
        } catch (SQLException e){
            throw new RuntimeException("Failed to register MeosSparkPostgresDriver.", e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        Connection connection = super.connect(url, info);
        if (connection instanceof PGConnection){
            for (MeosSparkDatatypes type: MeosSparkDatatypes.values()){
                type.registerType(type, (PGConnection) connection);
            }
        }
        return connection;
    }
}
