package org.mobiltydb.UDF.General;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.mobiltydb.UDT.MeosDatatype;
import org.mobiltydb.UDT.MeosDatatypeFactory;

import java.sql.SQLException;

public class GenericUDF {

    // Create a UDF that uses polymorphism to call the correct fromString method based on the subclass of MeosDatatype
    public static UDF2<String, String, byte[]> fromString = new UDF2<String, String, byte[]>() {
        @Override
        public byte[] call(String typeIdentifier, String str) {
            try {
                // Instantiate the appropriate MeosDatatype based on the identifier
                MeosDatatype<?> udt = MeosDatatypeFactory.createMeosDatatype(MeosDatatypeFactory.MeosTypes.valueOf(typeIdentifier));

                // Deserialize the string to the object using fromString
                Object obj = udt.fromString(str);
                // Serialize the object to a byte array using the serialize method of MeosDatatype
                return (byte[]) udt.serialize(obj);
            } catch (SQLException e) {
                throw new RuntimeException("Failed to convert string to custom type", e);
            }
        }
    };

    public static UDF1<byte[], String> deserializeToString = new UDF1<byte[], String>() {
        @Override
        public String call(byte[] bytes) {
            try {
                MeosDatatype<?> udt = MeosDatatypeFactory.createMeosDatatype(MeosDatatypeFactory.MeosTypes.PERIOD);
                return udt.deserialize(bytes).toString();
            } catch (Exception e) {
                throw new RuntimeException("Failed to convert binary to string representation", e);
            }
        }
    };

}
