package com.pellucid.aws.simpledb;

import java.util.List;
import java.util.ArrayList;

import org.junit.*;
import static org.junit.Assert.*;

import com.pellucid.aws.results.*;

public class SimpleDBTest {

    private final static SimpleDB sdb = new SimpleDB(SDBRegion.EU_WEST_1);

    @Test
    public void createAndDeleteDomain() {
        Result<SimpleDBMeta, Object> result = sdb.createDomain("java-delete-domain").get();
        assertTrue(result.toString(), result.isSuccess());
        Result<SimpleDBMeta, Object> result2 = sdb.deleteDomain("java-delete-domain").get();
        assertTrue(result2.toString(), result2.isSuccess());
    }

    @Test
    public void listDomains() {
        Result<SimpleDBMeta, Object> result = sdb.createDomain("java-list-domain").get();
        assertTrue(result.toString(), result.isSuccess());
        List<String> domainNames = sdb.listDomains().get().body();
        assertTrue("Couldn't find the created domain in the list", domainNames.contains("java-list-domain"));
        Result<SimpleDBMeta, Object> result2 = sdb.deleteDomain("java-list-domain").get();
        assertTrue(result2.toString(), result2.isSuccess());
    }

    @Test
    public void putDeleteAttributes() {
        List<SDBAttribute> attrs = new ArrayList<SDBAttribute>();
        attrs.add(new SDBAttribute("firstName", "toto"));
        attrs.add(new SDBAttribute("lastName", "tata"));
        Result<SimpleDBMeta, Object> resultc = sdb.createDomain("java-attrs").get();
        assertTrue(resultc.toString(), resultc.isSuccess());
        Result<SimpleDBMeta, Object> result = sdb.putAttributes("java-attrs", "foobar", attrs).get();
        assertTrue(result.toString(), result.isSuccess());
        Result<SimpleDBMeta, Object> result2 = sdb.deleteAttributes("java-attrs", "foobar", attrs).get();
        assertTrue(result.toString(), result2.isSuccess());
        Result<SimpleDBMeta, Object> resultd = sdb.deleteDomain("java-attrs").get();
        assertTrue(resultd.toString(), resultd.isSuccess());
    }

}