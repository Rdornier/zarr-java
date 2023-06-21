package com.scalableminds.zarrjava.v3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.scalableminds.zarrjava.v3.codec.Codec;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.function.Function;

public class Utils {

    public static ByteBuffer allocateNative(int capacity) {
        return ByteBuffer.allocate(capacity).order(ByteOrder.nativeOrder());
    }

    public static ByteBuffer makeByteBuffer(int capacity, Function<ByteBuffer, ByteBuffer> func) {
        ByteBuffer buf = allocateNative(capacity);
        buf = func.apply(buf);
        return (ByteBuffer) buf.rewind();
    }

    public static long[] toLongArray(int[] array) {
        return Arrays.stream(array).mapToLong(i -> (long) i).toArray();
    }

    public static int[] toIntArray(long[] array) {
        return Arrays.stream(array).mapToInt(i -> (int) i).toArray();
    }

    public static ObjectMapper makeObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new Jdk8Module());
        objectMapper.registerSubtypes(Codec.Registry.getNamedTypes());
        return objectMapper;
    }
}
