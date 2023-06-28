package com.scalableminds.zarrjava.v3;

import com.scalableminds.zarrjava.ZarrException;
import com.scalableminds.zarrjava.v3.chunkgrid.ChunkGrid;
import com.scalableminds.zarrjava.v3.chunkgrid.RegularChunkGrid;
import com.scalableminds.zarrjava.v3.chunkkeyencoding.ChunkKeyEncoding;
import com.scalableminds.zarrjava.v3.chunkkeyencoding.DefaultChunkKeyEncoding;
import com.scalableminds.zarrjava.v3.chunkkeyencoding.Separator;
import com.scalableminds.zarrjava.v3.chunkkeyencoding.V2ChunkKeyEncoding;
import com.scalableminds.zarrjava.v3.codec.Codec;
import com.scalableminds.zarrjava.v3.codec.CodecBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class ArrayMetadataBuilder {

  long[] shape = null;
  DataType dataType = null;
  ChunkGrid chunkGrid = null;
  ChunkKeyEncoding chunkKeyEncoding =
      new DefaultChunkKeyEncoding(new DefaultChunkKeyEncoding.Configuration(Separator.SLASH));

  Object fillValue = 0;
  Codec[] codecs = null;
  Map<String, Object> attributes = new HashMap<>();
  String[] dimensionNames = null;

  protected ArrayMetadataBuilder() {
  }

  protected static ArrayMetadataBuilder fromArrayMetadata(ArrayMetadata arrayMetadata) {
    ArrayMetadataBuilder builder = new ArrayMetadataBuilder();
    builder.shape = arrayMetadata.shape;
    builder.dataType = arrayMetadata.dataType;
    builder.chunkGrid = arrayMetadata.chunkGrid;
    builder.chunkKeyEncoding = arrayMetadata.chunkKeyEncoding;
    builder.fillValue = arrayMetadata.parsedFillValue;
    builder.codecs = arrayMetadata.codecs;
    builder.attributes = arrayMetadata.attributes;
    builder.dimensionNames = arrayMetadata.dimensionNames;
    return builder;
  }

  public ArrayMetadataBuilder withShape(long... shape) {
    this.shape = shape;
    return this;
  }

  public ArrayMetadataBuilder withDataType(DataType dataType) {
    this.dataType = dataType;
    return this;
  }

  public ArrayMetadataBuilder withDataType(String dataType) {
    this.dataType = DataType.valueOf(dataType);
    return this;
  }

  public ArrayMetadataBuilder withChunkShape(int... chunkShape) {
    this.chunkGrid = new RegularChunkGrid(new RegularChunkGrid.Configuration(chunkShape));
    return this;
  }

  public ArrayMetadataBuilder withDefaultChunkKeyEncoding(Separator separator) {
    this.chunkKeyEncoding = new DefaultChunkKeyEncoding(
        new DefaultChunkKeyEncoding.Configuration(separator));
    return this;
  }

  public ArrayMetadataBuilder withDefaultChunkKeyEncoding(String separator) {
    this.chunkKeyEncoding =
        new DefaultChunkKeyEncoding(
            new DefaultChunkKeyEncoding.Configuration(Separator.valueOf(separator)));
    return this;
  }

  public ArrayMetadataBuilder withV2ChunkKeyEncoding(Separator separator) {
    this.chunkKeyEncoding = new V2ChunkKeyEncoding(new V2ChunkKeyEncoding.Configuration(separator));
    return this;
  }

  public ArrayMetadataBuilder withV2ChunkKeyEncoding(String separator) {
    this.chunkKeyEncoding =
        new V2ChunkKeyEncoding(new V2ChunkKeyEncoding.Configuration(Separator.valueOf(separator)));
    return this;
  }

  public ArrayMetadataBuilder withFillValue(Object fillValue) {
    this.fillValue = fillValue;
    return this;
  }

  public ArrayMetadataBuilder withCodecs(Codec... codecs) {
    this.codecs = codecs;
    return this;
  }

  public ArrayMetadataBuilder withCodecs(Function<CodecBuilder, CodecBuilder> codecBuilder) {
    if (dataType==null) {
      throw new IllegalStateException("Please call `withDataType` first.");
    }
    CodecBuilder nestedCodecBuilder = new CodecBuilder(dataType);
    this.codecs = codecBuilder.apply(nestedCodecBuilder)
        .build();
    return this;
  }

  public ArrayMetadataBuilder withDimensionNames(String... dimensionNames) {
    this.dimensionNames = dimensionNames;
    return this;
  }

  public ArrayMetadataBuilder putAttribute(String key, Object value) {
    this.attributes.put(key, value);
    return this;
  }

  public ArrayMetadataBuilder withAttributes(Map<String, Object> attributes) {
    this.attributes = attributes;
    return this;
  }

  public ArrayMetadata build() throws ZarrException {
    if (shape == null) {
      throw new ZarrException("Shape needs to be provided. Please call `.withShape`.");
    }
    if (dataType == null) {
      throw new ZarrException("Data type needs to be provided. Please call `.withDataType`.");
    }
    if (chunkGrid == null) {
      throw new ZarrException("Chunk grid needs to be provided. Please call `.withChunkShape`.");
    }
    if (chunkGrid instanceof RegularChunkGrid
        && shape.length != ((RegularChunkGrid) chunkGrid).configuration.chunkShape.length) {
      throw new ZarrException("Shape (ndim=" + shape.length + ") and chunk shape (ndim=" +
          ((RegularChunkGrid) chunkGrid).configuration.chunkShape.length +
          ") need to have the same number of dimensions.");
    }
    return new ArrayMetadata(shape, dataType, chunkGrid, chunkKeyEncoding, fillValue, codecs,
        dimensionNames,
        attributes
    );
  }
}
