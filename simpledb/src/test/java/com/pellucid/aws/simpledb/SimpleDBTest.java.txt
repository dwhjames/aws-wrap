package com.pellucid.aws.simpledb;

import akka.dispatch.*;
import scala.concurrent.duration.Duration;
import scala.concurrent.Await;

import java.util.List;
import java.util.ArrayList;

import org.junit.*;
import static org.junit.Assert.*;

import com.pellucid.aws.results.*;

public class SimpleDBTest {

    private final static Duration timeout = Duration.create("30 seconds");

    private final static SimpleDB sdb = new SimpleDB(SDBRegion.EU_WEST_1);

    @Test
    public void createAndDeleteDomain() throws Exception {
        Result<SimpleDBMeta, Object> result = Await.result(sdb.createDomain("java-delete-domain"), timeout);
        assertTrue(result.toString(), result.isSuccess());
        Result<SimpleDBMeta, Object> result2 = Await.result(sdb.deleteDomain("java-delete-domain"), timeout);
        assertTrue(result2.toString(), result2.isSuccess());
    }

    @Test
    public void domainMetadata() throws Exception {
        Result<SimpleDBMeta, Object> result = Await.result(sdb.createDomain("java-delete-domain"), timeout);
        assertTrue(result.toString(), result.isSuccess());
        Result<SimpleDBMeta, SDBDomainMetadata> resultm = Await.result(sdb.domainMetadata("java-delete-domain"), timeout);
        assertTrue(resultm.toString(), resultm.isSuccess());
        Result<SimpleDBMeta, Object> result2 = Await.result(sdb.deleteDomain("java-delete-domain"), timeout);
        assertTrue(result2.toString(), result2.isSuccess());
    }

    @Test
    public void listDomains() throws Exception {
        Result<SimpleDBMeta, Object> result = Await.result(sdb.createDomain("java-list-domain"), timeout);
        assertTrue(result.toString(), result.isSuccess());
        List<String> domainNames = Await.result(sdb.listDomains(), timeout).body();
        assertTrue("Couldn't find the created domain in the list", domainNames.contains("java-list-domain"));
        Result<SimpleDBMeta, Object> result2 = Await.result(sdb.deleteDomain("java-list-domain"), timeout);
        assertTrue(result2.toString(), result2.isSuccess());
    }

    @Test
    public void putDeleteAttributes() throws Exception {
        List<SDBAttribute> attrs = new ArrayList<SDBAttribute>();
        attrs.add(new SDBAttribute("firstName", "toto"));
        attrs.add(new SDBAttribute("lastName", "tata"));
        Result<SimpleDBMeta, Object> resultc = Await.result(sdb.createDomain("java-attrs"), timeout);
        assertTrue(resultc.toString(), resultc.isSuccess());
        Result<SimpleDBMeta, Object> result = Await.result(sdb.putAttributes("java-attrs", "foobar", attrs), timeout);
        assertTrue(result.toString(), result.isSuccess());
        Result<SimpleDBMeta, List<SDBAttribute>> resultGet = Await.result(sdb.getAttributes("java-attrs", "foobar", true), timeout);
        boolean found = false;
        for (SDBAttribute attr: resultGet.body()) {
            if ("firstName".equals(attr.name()) && "toto".equals(attr.value())) found = true;
        }
        assertTrue("Couldn't find the inserted attribute", found);
        Result<SimpleDBMeta, Object> result2 = Await.result(sdb.deleteAttributes("java-attrs", "foobar", attrs), timeout);
        assertTrue(result.toString(), result2.isSuccess());
        Result<SimpleDBMeta, Object> resultd = Await.result(sdb.deleteDomain("java-attrs"), timeout);
        assertTrue(resultd.toString(), resultd.isSuccess());
    }

    @Test
    public void select() throws Exception {
        List<SDBAttribute> attrs = new ArrayList<SDBAttribute>();
        attrs.add(new SDBAttribute("firstName", "toto"));
        attrs.add(new SDBAttribute("lastName", "tata"));
        Result<SimpleDBMeta, Object> resultc = Await.result(sdb.createDomain("java-select"), timeout);
        assertTrue(resultc.toString(), resultc.isSuccess());
        Result<SimpleDBMeta, Object> result = Await.result(sdb.putAttributes("java-select", "foobar", attrs), timeout);
        assertTrue(result.toString(), result.isSuccess());

        Result<SimpleDBMeta, List<SDBItem>> resultSelect = Await.result(sdb.select("select * from `java-select`", true), timeout);
        assertTrue(resultSelect.toString(), resultSelect.isSuccess());
        assertTrue("Empty select response, should have at least 1 element", resultSelect.body().size() > 0);
        boolean found = false;
        for (SDBItem item: resultSelect.body()) {
            if ("foobar".equals(item.name())) found = true;
        }
        assertTrue("Couldn't find the inserted item", found);

        Result<SimpleDBMeta, Object> resultd = Await.result(sdb.deleteDomain("java-attrs"), timeout);
        assertTrue(resultd.toString(), resultd.isSuccess());
    }

}