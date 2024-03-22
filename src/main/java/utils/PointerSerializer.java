package utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import functions.functions;
import jnr.ffi.Pointer;

public class PointerSerializer extends Serializer<Pointer> {

    @Override
    public void write(Kryo kryo, Output output, Pointer pointer) {
        // Convert the Pointer to its String representation.
        String periodStr = functions.period_out(pointer);
        output.writeString(periodStr);
    }

    @Override
    public Pointer read(Kryo kryo, Input input, Class<Pointer> type) {
        // Rebuild the Pointer from its String representation.
        String periodStr = input.readString();
        // Assuming you have a reverse function to period_out to convert the string back to a Pointer.
        Pointer pointer = functions.period_in(periodStr);
        return pointer;
    }
}