package dev.zarr.zarrjava.v2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.ZarrException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class ArrayMetadata {
  static final int ZARR_FORMAT = 2;

  @JsonProperty("zarr_format")
  public final int zarrFormat = ZARR_FORMAT;

  @JsonProperty("shape")
  public final long[] shape;

  @JsonProperty("dtype")
  public DataType dataType;

  @JsonProperty("chunks")
  public final long[] chunks;

  @JsonProperty("fill_value")
  public final Object fillValue;

  @JsonIgnore
  public final Object parsedFillValue;

  @JsonProperty("compressor")
  public final Codec compressor;

  @JsonProperty("order")
  public final Order order;

  @JsonProperty("filters")
  public Codec[] filters;

  @Nullable
  @JsonProperty("dimension_separator")
  public String[] dimensionSeparator;

  @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
  public ArrayMetadata(
          @JsonProperty(value = "zarr_format", required = true) int zarrFormat,
          @JsonProperty(value = "shape", required = true) long[] shape,
          @JsonProperty(value = "dtype", required = true) DataType dataType,
          @JsonProperty(value = "chunks", required = true) long[] chunks,
          @JsonProperty(value = "fill_value", required = true) Object fillValue,
          @JsonProperty(value = "compressor", required = true) Codec compressor,
          @JsonProperty(value = "order", required = true) Order order,
          @JsonProperty(value = "filters") Codec[] filters,
          @Nullable @JsonProperty(value = "dimension_separator") String[] dimensionSeparator
  ) throws ZarrException {
    if (zarrFormat != this.zarrFormat) {
      throw new ZarrException(
              "Expected zarr format '" + this.zarrFormat + "', got '" + zarrFormat + "'.");
    }

    this.shape = shape;
    this.dataType = dataType;
    this.fillValue = fillValue;
    this.parsedFillValue = parseFillValue(fillValue, dataType);
    this.chunks = chunks;
    this.order = order;
    this.compressor = compressor;
    this.filters = filters;
    this.dimensionSeparator = dimensionSeparator;
  }

  public ArrayMetadata(
          long[] shape,
          DataType dataType,
          long[] chunks,
          Object fillValue,
          Codec compressor,
          Order order,
          Codec[] filters,
          @Nullable String[] dimensionSeparator
  ) throws ZarrException {
    this(ZARR_FORMAT, shape, dataType, chunks, fillValue, compressor, order, filters, dimensionSeparator);
  }

  //TODO check if it meets the specs
  public static Object parseFillValue(Object fillValue, @Nonnull DataType dataType)
          throws ZarrException {
    if (fillValue instanceof Number) {
      Number fillValueNumber = (Number) fillValue;
      switch (dataType) {
        case BOOL:
          return fillValueNumber.byteValue() != 0;
        case INT8:
        case UINT8:
          return fillValueNumber.byteValue();
        case INT16:
        case UINT16:
          return fillValueNumber.shortValue();
        case INT32:
        case UINT32:
          return fillValueNumber.intValue();
        case INT64:
        case UINT64:
          return fillValueNumber.longValue();
        case FLOAT32:
          return fillValueNumber.floatValue();
        case FLOAT64:
          return fillValueNumber.doubleValue();
        default:
          // Fallback to throwing below
      }
    } else if (fillValue instanceof String) {
      String fillValueString = (String) fillValue;
      if (fillValueString.equals("NaN")) {
        switch (dataType) {
          case FLOAT32:
            return Float.NaN;
          case FLOAT64:
            return Double.NaN;
          default:
            throw new ZarrException(
                    "Invalid fill value '" + fillValueString + "' for data type '" + dataType + "'.");
        }
      } else if (fillValueString.equals("+Infinity")) {
        switch (dataType) {
          case FLOAT32:
            return Float.POSITIVE_INFINITY;
          case FLOAT64:
            return Double.POSITIVE_INFINITY;
          default:
            throw new ZarrException(
                    "Invalid fill value '" + fillValueString + "' for data type '" + dataType + "'.");
        }
      } else if (fillValueString.equals("-Infinity")) {
        switch (dataType) {
          case FLOAT32:
            return Float.NEGATIVE_INFINITY;
          case FLOAT64:
            return Double.NEGATIVE_INFINITY;
          default:
            throw new ZarrException(
                    "Invalid fill value '" + fillValueString + "' for data type '" + dataType + "'.");
        }
      }
    }
    throw new ZarrException("Invalid fill value '" + fillValue + "'.");
  }
}
