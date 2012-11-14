package com.pellucid.aws.simpledb;

import org.junit.*;
import static org.junit.Assert.*;

import com.pellucid.aws.results.*;

public class SimpleDBTest {

    private final static SimpleDB sdb = new SimpleDB(SDBRegion.EU_WEST_1);

    @Test
    public void createDomain() {
        Result<SimpleDBMeta, Object> result = sdb.createDomain("java-create-domain").get();
        assertTrue(result.isSuccess());
    }

    @Test
    public void deleteDomain() {
        Result<SimpleDBMeta, Object> result = sdb.createDomain("java-delete-domain").get();
        assertTrue(result.isSuccess());
        Result<SimpleDBMeta, Object> result2 = sdb.deleteDomain("java-delete-domain").get();
        assertTrue(result2.isSuccess());
    }

}