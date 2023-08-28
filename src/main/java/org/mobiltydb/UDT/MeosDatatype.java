package org.mobiltydb.UDT;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.UserDefinedType;
import org.apache.spark.unsafe.types.UTF8String;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.postgresql.util.PGobject;
import utils.TypeName;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MeosDatatype is an abstract class extending Spark's UserDefinedType.
 * It provides a generic framework for serializing and deserializing custom data types using Kryo.
 *
 * @param <T> The type of object this UDT represents.
 */
public abstract class MeosDatatype<T> extends UserDefinedType<T>{

    private static final Logger logger = LoggerFactory.getLogger(MeosDatatype.class);

    private PGobject pgObject = new PGobject();

    public void setValue(String value) throws SQLException {
        pgObject.setValue(value);
    }

//    public String getValue() {
//        return pgObject.getValue();
//    }

    public String getType() {
        return pgObject.getType();
    }

    public String getTypeNameFromAnnotation() {
        TypeName typeNameAnnotation = this.getClass().getAnnotation(TypeName.class);
        if (typeNameAnnotation != null) {
            return typeNameAnnotation.name();
        }
        return null; // or throw an exception if you want it to be mandatory
    }

    public MeosDatatype() {
        try {
            pgObject.setType(getTypeNameFromAnnotation());
        } catch (Exception e) {
            throw new RuntimeException("Failed to set PGobject type", e);
        }
    }

    // ThreadLocal instance of Kryo for thread safety.
    // Ensures each thread has its own Kryo instance.
    private static final ThreadLocal<Kryo> kryoThreadLocal = ThreadLocal.withInitial(() -> {
        Kryo kryo = new Kryo();

        // Set the instantiator strategy for Kryo
        kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));

        // Registering the String class with Kryo
        kryo.register(String.class);
        return kryo;
    });

    // SQL representation for custom data types is Binary.
    private static final DataType SQL_TYPE = DataTypes.StringType;

    /**
     * Provides the SQL data type's schema corresponding to this UDT.
     * @return The SQL data type.
     */
    @Override
    public DataType sqlType(){
        return SQL_TYPE;
    }

    /**
     * Serializes the object into a byte array using Kryo.
     *
     * @param obj The object to serialize.
     * @return The serialized object as a byte array.
     */
//    @Override
//    public Object serialize(Object obj){
//        Kryo kryo = kryoThreadLocal.get();
//        ByteArrayOutputStream baos = new ByteArrayOutputStream();
//        Output output = new Output(baos);
//
//        // Convert the object to a string representation and serialize with Kryo
//        String objString = obj.toString();
//        kryo.writeObject(output, objString);
//
//        output.close();
//        return baos.toByteArray();
//    }

    @Override
    public Object serialize(Object obj) {
        return UTF8String.fromString(obj.toString());
    }


    /**
     * Deserializes a byte array into an object using Kryo.
     *
     * @param datum The serialized data.
     * @return The deserialized object.
     */
//    @Override
//    public T deserialize(Object datum){
//        // Check if the incoming data is of the expected type
//        if (!(datum instanceof byte[] bytes)) {
//            throw new IllegalArgumentException("Expected byte[], but got: " + datum.getClass().getSimpleName());
//        }
//
//        Kryo kryo = kryoThreadLocal.get();
//        Input input = new Input(new ByteArrayInputStream(bytes));
//
//        // Deserialize the byte array to a string representation of the object
//        String objString = kryo.readObject(input, String.class);
//        input.close();
//
//        // Convert the string back to the object
//        try{
//            return fromString(objString);
//        } catch(SQLException e){
//            throw new RuntimeException(e);
//        }
//    }

    @Override
    public T deserialize(Object datum) {
        String objString;
        if (datum instanceof UTF8String) {
            objString = ((UTF8String) datum).toString();
        } else if (datum instanceof String) {
            objString = (String) datum;
        } else {
            throw new IllegalArgumentException("Expected UTF8String or String, but got: " + datum.getClass().getSimpleName());
        }
        logger.debug("Deserializing: {}", datum);
        try {
            return fromString(objString);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Abstract method to be implemented by subclasses.
     * It converts a string representation back to the object.
     *
     * @param s The string representation of the object.
     * @return The deserialized object.
     * @throws SQLException If there's an issue with the conversion.
     */
    public abstract T fromString(String s) throws SQLException;

}
