package com.scalableminds.zarrjava;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalableminds.zarrjava.store.FilesystemStore;
import com.scalableminds.zarrjava.store.HttpStore;
import com.scalableminds.zarrjava.store.S3Store;
import com.scalableminds.zarrjava.v3.*;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class ZarrTest {

    @Test
    public void testStores() throws IOException {
        FilesystemStore fsStore = new FilesystemStore("");
        HttpStore httpStore = new HttpStore("https://static.webknossos.org/data");
        S3Store s3Store = new S3Store(AmazonS3ClientBuilder.standard().withRegion("eu-west-1").withCredentials(
                new ProfileCredentialsProvider()).build(), "static.webknossos.org", "data");

        ObjectMapper objectMapper = Utils.makeObjectMapper();

        GroupMetadata group = objectMapper.readValue(new File("l4_sample_no_sharding/zarr.json"), GroupMetadata.class);

        System.out.println(group);
        System.out.println(objectMapper.writeValueAsString(group));

        ArrayMetadata arrayMetadata =
                objectMapper.readValue(new File("l4_sample_no_sharding/color/1/zarr.json"), ArrayMetadata.class);

        System.out.println(arrayMetadata);
        System.out.println(objectMapper.writeValueAsString(arrayMetadata));


        System.out.println(new Array(fsStore, "l4_sample_no_sharding/color/1"));
        // System.out.println(new Group(fsStore, "l4_sample_no_sharding").list());
        // System.out.println(((Group) new Group(fsStore, "l4_sample_no_sharding").get("color")).list());

        System.out.println(new com.scalableminds.zarrjava.v2.Array(httpStore, "l4_sample/color/1"));

        System.out.println(new Array(httpStore, "zarr_v3/l4_sample/color/1"));
        System.out.println(new Array(s3Store, "zarr_v3/l4_sample/color/1"));

    }

    @Test
    public void testV3() throws IOException {
        FilesystemStore fsStore = new FilesystemStore("");

        Array array = new Array(fsStore, "l4_sample/color/1");

        ucar.ma2.Array outArray = array.read(new long[]{0, 3073, 3073, 513}, new int[]{1, 64, 64, 64});
        assert outArray.getSize() == 64 * 64 * 64;
        assert outArray.getByte(0) == -98;
        System.out.println(outArray.toString());
    }

    @Test
    public void testV3FillValue() {
        assert Arrays.equals(ArrayMetadata.getFillValueBytes(0, DataType.UINT32).array(), new byte[]{0, 0, 0, 0});
        assert Arrays.equals(ArrayMetadata.getFillValueBytes("0x00010203", DataType.UINT32).array(),
                new byte[]{0, 1, 2, 3});
        assert Arrays.equals(ArrayMetadata.getFillValueBytes("0b00000010", DataType.UINT8).array(), new byte[]{2});
        assert Double.isNaN(ArrayMetadata.getFillValueBytes("NaN", DataType.FLOAT64).getDouble());
        assert Double.isInfinite(ArrayMetadata.getFillValueBytes("-Infinity", DataType.FLOAT64).getDouble());
    }

    @Test
    public void testV2() throws IOException {

        FilesystemStore fsStore = new FilesystemStore("");
        HttpStore httpStore = new HttpStore("https://static.webknossos.org/data");

        System.out.println(new com.scalableminds.zarrjava.v2.Array(httpStore, "l4_sample/color/1"));

        //System.out.println(new Array(fsStore, "l4_sample_no_sharding/color/1").read(new long[]{3072, 3072, 512, 1},
        //        new int[]{64, 64, 64, 1}).length);


    }
}