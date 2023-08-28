package utils;

import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcType;
import org.apache.spark.sql.jdbc.PostgresDialect;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StringType;
import org.mobiltydb.UDT.MeosDatatype;
import org.mobiltydb.UDT.MeosDatatypeFactory;
import org.mobiltydb.UDT.PeriodUDT;
import org.postgresql.util.PGobject;
import scala.Option;

import javax.xml.crypto.Data;
import java.sql.JDBCType;
import java.sql.Types;
import java.util.Optional;

import static scala.Console.print;

public class MobilityDBJdbcDialect extends JdbcDialect{

    @Override
    public boolean canHandle(String url){
        return url.startsWith("jdbc:postgresql:");
    }

    @Override
    public Option<DataType> getCatalystType(int sqlType, String typeName, int size, MetadataBuilder md) {
        MeosSparkDatatypes type = MeosSparkDatatypes.fromTypeName(typeName);
        try {
            assert type != null;
            Class<?> udtClass = type.getSparkUdtClass();
            Class<?> meosClass = type.getMeosClass();

            DataType udtType = (DataType) udtClass.newInstance();
            System.out.println(sqlType + " " + typeName + " " + size + " "+ md + udtType + (udtType instanceof MeosDatatype<?>));
            if (sqlType == java.sql.Types.OTHER && udtType instanceof MeosDatatype<?>) {
                return Option.apply(DataTypes.StringType);
            }else{
                return PostgresDialect.getCatalystType(sqlType, typeName, size, md);
            }
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    //    @Override
//    public Option<JdbcType> getJDBCType(DataType dt) {
//        if (dt instanceof MeosDatatype<?>) {
//            MeosSparkDatatypes type = MeosSparkDatatypes.fromDataType(dt);
//            String meosType = ((MeosDatatype<?>) dt).getTypeNameFromAnnotation();
//            Class<?> udtClass = type.getSparkUdtClass();
//            Class<?> meosClass = type.getMeosClass();
//            System.out.println(meosType);
//            JdbcType jdbcType = new JdbcType("VARCHAR", Types.VARCHAR);
//            Option<JdbcType> option = Option.apply(jdbcType);
//            return option;
//        }
//        return PostgresDialect.getJDBCType(dt);
//    }

}
