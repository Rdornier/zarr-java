package dev.zarr.zarrjava;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.HttpStore;
import dev.zarr.zarrjava.store.S3Store;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.utils.MultiArrayUtils;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.ArrayMetadata;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.v3.Group;
import dev.zarr.zarrjava.v3.GroupMetadata;
import dev.zarr.zarrjava.v3.Node;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import dev.zarr.zarrjava.v3.codec.CodecBuilder;
import org.junit.Before;
import org.junit.Test;

import javax.xml.crypto.Data;


public class ZarrTest {

  final Path TESTDATA = Paths.get("testdata");
  final Path TESTOUTPUT = Paths.get("testoutput");
  final Path ZARRITA = Paths.get("src\\test\\java\\dev\\zarr\\zarrjava\\run_zarrita.py");
  final String CONDA_ENVIRONMENT = "zarrita_env";

  @Before
  public void clearTestoutputFolder() throws IOException {
    if (Files.exists(TESTOUTPUT)) {
      try (Stream<Path> walk = Files.walk(TESTOUTPUT)) {
        walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
      }
    }
    Files.createDirectory(TESTOUTPUT);
  }

  @Before
  public void installZarritaInCondaEnv() throws IOException {
    Process process = Runtime.getRuntime().exec("conda run -n "+CONDA_ENVIRONMENT+" pip install zarrita");

    BufferedReader stdError = new BufferedReader(new InputStreamReader(process.getErrorStream()));
    String s;
    boolean environmentLocationNotFound = false;
    while ((s = stdError.readLine()) != null) {
      System.err.println(s);
      if(s.contains("EnvironmentLocationNotFound")){
        environmentLocationNotFound = true;

      }
    }
    if (environmentLocationNotFound){
      System.out.println("creating conda environment: " + CONDA_ENVIRONMENT);
      Runtime.getRuntime().exec("conda create --name "+CONDA_ENVIRONMENT+" -y");
      Runtime.getRuntime().exec("conda run -n "+CONDA_ENVIRONMENT+" pip install zarrita");
    }
    while ((s = stdError.readLine()) != null) {
      System.err.println(s);
    }
  }


  @Test
  public void testReadFromZarrita() throws IOException, ZarrException {

    String zarritaCommand = "conda run -n "+CONDA_ENVIRONMENT+" python " + ZARRITA;
    Process process = Runtime.getRuntime().exec(zarritaCommand);
    System.out.println(zarritaCommand);

    BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
    BufferedReader stdError = new BufferedReader(new InputStreamReader(process.getErrorStream()));
    String s;
    System.out.println("out: ");
    while ((s = stdInput.readLine()) != null) {
      System.out.println(s);
    }
    System.out.println("err:");
    while ((s = stdError.readLine()) != null) {
      System.err.println(s);
    }

    Array array = Array.open(new FilesystemStore(TESTOUTPUT).resolve("array"));

    ucar.ma2.Array result = array.read();


    //for expected values see run_zarrita.py
    assertArrayEquals(new int[] {16, 16}, result.getShape());
    assertEquals(DataType.INT32, array.metadata.dataType);
    assertArrayEquals(new int[]{2, 8}, array.metadata.chunkShape());
    assertEquals(42, array.metadata.attributes.get("answer"));
    int[] expectedData = new int[16 * 16];
    for (int i = 0; i < 16 * 16; i++) {
      expectedData[i] = i;
    }
    assertArrayEquals(expectedData, (int[]) result.get1DJavaArray(ucar.ma2.DataType.INT));

  @Test
  public void testFileSystemStores() throws IOException, ZarrException {
    FilesystemStore fsStore = new FilesystemStore(TESTDATA);
    ObjectMapper objectMapper = Node.makeObjectMapper();

    GroupMetadata group = objectMapper.readValue(
        Files.readAllBytes(TESTDATA.resolve("l4_sample").resolve("zarr.json")),
        GroupMetadata.class
    );

    System.out.println(group);
    System.out.println(objectMapper.writeValueAsString(group));

    ArrayMetadata arrayMetadata = objectMapper.readValue(Files.readAllBytes(TESTDATA.resolve(
            "l4_sample").resolve("color").resolve("1").resolve("zarr.json")),
        ArrayMetadata.class);

    System.out.println(arrayMetadata);
    System.out.println(objectMapper.writeValueAsString(arrayMetadata));

    System.out.println(
        Array.open(fsStore.resolve("l4_sample", "color", "1")));
    System.out.println(
        Arrays.toString(Group.open(fsStore.resolve("l4_sample")).list().toArray(Node[]::new)));
    System.out.println(
        Arrays.toString(((Group) Group.open(fsStore.resolve("l4_sample")).get("color")).list()
            .toArray(Node[]::new)));
  }

  @Test
  public void testS3Store() throws IOException, ZarrException {
    S3Store s3Store = new S3Store(AmazonS3ClientBuilder.standard()
        .withRegion("eu-west-1")
        .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
        .build(), "static.webknossos.org", "data");
    System.out.println(Array.open(s3Store.resolve("zarr_v3", "l4_sample", "color", "1")));
  }

  @Test
  public void testHttpStore() throws IOException, ZarrException {
    HttpStore httpStore = new HttpStore("https://static.webknossos.org/data/");
    System.out.println(
        dev.zarr.zarrjava.v2.Array.open(httpStore.resolve("l4_sample", "color", "1")));
    System.out.println(Array.open(httpStore.resolve("zarr_v3", "l4_sample", "color", "1")));
  }

  @Test
  public void testV3ShardingReadCutout() throws IOException, ZarrException {
    Array array = Array.open(new FilesystemStore(TESTDATA).resolve("l4_sample", "color", "1"));

    ucar.ma2.Array outArray = array.read(new long[]{0, 3073, 3073, 513}, new int[]{1, 64, 64, 64});
    assertEquals(outArray.getSize(), 64 * 64 * 64);
    assertEquals(outArray.getByte(0), -98);
  }

  @Test
  public void testV3Access() throws IOException, ZarrException {
    Array readArray = Array.open(new FilesystemStore(TESTDATA).resolve("l4_sample", "color", "1"));

    ucar.ma2.Array outArray = readArray.access().withOffset(0, 3073, 3073, 513)
        .withShape(1, 64, 64, 64)
        .read();
    assertEquals(outArray.getSize(), 64 * 64 * 64);
    assertEquals(outArray.getByte(0), -98);

    Array writeArray = Array.create(
        new FilesystemStore(TESTOUTPUT).resolve("l4_sample_2", "color", "1"),
        readArray.metadata
    );
    writeArray.access().withOffset(0, 3073, 3073, 513).write(outArray);
  }

  @Test
  public void testV3ShardingReadWrite() throws IOException, ZarrException {
    Array readArray = Array.open(
        new FilesystemStore(TESTDATA).resolve("l4_sample", "color", "8-8-2"));
    ucar.ma2.Array readArrayContent = readArray.read();
    Array writeArray = Array.create(
        new FilesystemStore(TESTOUTPUT).resolve("l4_sample_3", "color", "8-8-2"),
        readArray.metadata
    );
    writeArray.write(readArrayContent);
    ucar.ma2.Array outArray = writeArray.read();

    assert MultiArrayUtils.allValuesEqual(outArray, readArrayContent);
  }

  @Test
  public void testV3Codecs() throws IOException, ZarrException {
    Array readArray = Array.open(
        new FilesystemStore(TESTDATA).resolve("l4_sample", "color", "8-8-2"));
    ucar.ma2.Array readArrayContent = readArray.read();
    {
      Array gzipArray = Array.create(
          new FilesystemStore(TESTOUTPUT).resolve("l4_sample_gzip", "color", "8-8-2"),
          Array.metadataBuilder(readArray.metadata).withCodecs(c -> c.withGzip(5)).build()
      );
      gzipArray.write(readArrayContent);
      ucar.ma2.Array outGzipArray = gzipArray.read();
      assert MultiArrayUtils.allValuesEqual(outGzipArray, readArrayContent);
    }
    {
      Array bloscArray = Array.create(
          new FilesystemStore(TESTOUTPUT).resolve("l4_sample_blosc", "color", "8-8-2"),
          Array.metadataBuilder(readArray.metadata).withCodecs(c -> c.withBlosc("zstd", 5)).build()
      );
      bloscArray.write(readArrayContent);
      ucar.ma2.Array outBloscArray = bloscArray.read();
      assert MultiArrayUtils.allValuesEqual(outBloscArray, readArrayContent);
    }
    {
      Array zstdArray = Array.create(
          new FilesystemStore(TESTOUTPUT).resolve("l4_sample_zstd", "color", "8-8-2"),
          Array.metadataBuilder(readArray.metadata).withCodecs(c -> c.withZstd(10)).build()
      );
      zstdArray.write(readArrayContent);
      ucar.ma2.Array outZstdArray = zstdArray.read();
      assert MultiArrayUtils.allValuesEqual(outZstdArray, readArrayContent);
    }
  }

  @Test
  public void testV3ArrayMetadataBuilder() throws ZarrException {
    Array.metadataBuilder()
        .withShape(1, 4096, 4096, 1536)
        .withDataType(DataType.UINT32)
        .withChunkShape(1, 1024, 1024, 1024)
        .withFillValue(0)
        .withCodecs(
            c -> c.withSharding(new int[]{1, 32, 32, 32}, c1 -> c1.withBlosc()))
        .build();
  }

  @Test
  public void testV3FillValue() throws ZarrException {
    assertEquals((int) ArrayMetadata.parseFillValue(0, DataType.UINT32), 0);
    assertEquals((int) ArrayMetadata.parseFillValue("0x00010203", DataType.UINT32), 50462976);
    assertEquals((byte) ArrayMetadata.parseFillValue("0b00000010", DataType.UINT8), 2);
    assert Double.isNaN((double) ArrayMetadata.parseFillValue("NaN", DataType.FLOAT64));
    assert Double.isInfinite((double) ArrayMetadata.parseFillValue("-Infinity", DataType.FLOAT64));
  }

  @Test
  public void testV3Group() throws IOException, ZarrException {
    FilesystemStore fsStore = new FilesystemStore(TESTOUTPUT);

    Group group = Group.create(fsStore.resolve("testgroup"));
    Group group2 = group.createGroup("test2", new HashMap<String, Object>() {{
      put("hello", "world");
    }});
    Array array = group2.createArray("array", b ->
        b.withShape(10, 10)
            .withDataType(DataType.UINT8)
            .withChunkShape(5, 5)
    );
    array.write(new long[]{2, 2}, ucar.ma2.Array.factory(ucar.ma2.DataType.UBYTE, new int[]{8, 8}));

    assertArrayEquals(
        ((Array) ((Group) group.listAsArray()[0]).listAsArray()[0]).metadata.chunkShape(),
        new int[]{5, 5});
  }

  @Test
  public void testV2() throws IOException, ZarrException {
    FilesystemStore fsStore = new FilesystemStore("");
    HttpStore httpStore = new HttpStore("https://static.webknossos.org/data");

    System.out.println(
        dev.zarr.zarrjava.v2.Array.open(httpStore.resolve("l4_sample", "color", "1")));
  }
}
