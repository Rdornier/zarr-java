package com.scalableminds.zarrjava.store;

import com.scalableminds.zarrjava.indexing.OpenSlice;
import com.scalableminds.zarrjava.v3.DataType;
import ucar.ma2.Array;

import java.nio.ByteBuffer;

public class BufferValueHandle extends ValueHandle {

    ByteBuffer bytes;

    public BufferValueHandle(ByteBuffer bytes) {
        this.bytes = bytes;
    }

    public BufferValueHandle(byte[] bytes) {
        this.bytes = ByteBuffer.wrap(bytes);
    }

    @Override
    public ValueHandle get(OpenSlice slice) {
        return null;
    }

    @Override
    public void set(OpenSlice slice, ValueHandle value) {

    }

    @Override
    public ByteBuffer toBytes() {
        return bytes;
    }

    @Override
    public Array toArray() {
        return null;
    }

    @Override
    public Array toArray(int[] shape, DataType dataType) {
        return Array.factory(dataType.getMA2DataType(), shape, bytes);
    }
}
