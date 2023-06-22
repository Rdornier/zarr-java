package com.scalableminds.zarrjava.v3.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.scalableminds.zarrjava.v3.ArrayMetadata;
import com.scalableminds.zarrjava.v3.codec.ArrayBytesCodec;
import ucar.ma2.Array;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class EndianCodec implements ArrayBytesCodec {
    public final String name = "endian";
    @Nonnull
    public final Configuration configuration;

    @JsonCreator
    public EndianCodec(
            @Nonnull @JsonProperty(value = "configuration", required = true) Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Array decode(ByteBuffer chunkBytes, ArrayMetadata.CoreArrayMetadata arrayMetadata) {
        chunkBytes.order(configuration.endian.getByteOrder());
        return Array.factory(arrayMetadata.dataType.getMA2DataType(), arrayMetadata.chunkShape, chunkBytes);
    }

    @Override
    public ByteBuffer encode(Array chunkArray, ArrayMetadata.CoreArrayMetadata arrayMetadata) {
        return chunkArray.getDataAsByteBuffer(configuration.endian.getByteOrder());
    }

    public enum Endian {
        LITTLE("little"), BIG("big");
        private final String endian;

        Endian(String endian) {
            this.endian = endian;
        }

        @JsonValue
        public String getValue() {
            return endian;
        }

        public ByteOrder getByteOrder() {
            switch (this) {
                case LITTLE:
                    return ByteOrder.LITTLE_ENDIAN;
                case BIG:
                    return ByteOrder.BIG_ENDIAN;
                default:
                    throw new RuntimeException("Unreachable");
            }
        }
    }

    public static final class Configuration {
        @Nonnull
        public final EndianCodec.Endian endian;

        @JsonCreator
        public Configuration(@JsonProperty(value = "endian", defaultValue = "little") EndianCodec.Endian endian) {
            this.endian = endian;
        }
    }
}
