/*package com.pellucid.aws.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import com.pellucid.aws.dynamodb.models.AttributeValue;
import com.pellucid.aws.dynamodb.models.BatchGetResponse;
import com.pellucid.aws.dynamodb.models.BatchWriteResponse;
import com.pellucid.aws.dynamodb.models.GetRequest;
import com.pellucid.aws.dynamodb.models.ItemResponse;
import com.pellucid.aws.dynamodb.models.KeySchemaElement;
import com.pellucid.aws.dynamodb.models.KeySchemaElement.AttributeType;
import com.pellucid.aws.dynamodb.models.KeyValue;
import com.pellucid.aws.dynamodb.models.PrimaryKey;
import com.pellucid.aws.dynamodb.models.ProvisionedThroughput;
import com.pellucid.aws.dynamodb.models.Query;
import com.pellucid.aws.dynamodb.models.QueryResponse;
import com.pellucid.aws.dynamodb.models.TableDescription;
import com.pellucid.aws.dynamodb.models.TableStatus;
import com.pellucid.aws.dynamodb.models.WriteRequest;
import com.pellucid.aws.results.SimpleResult;

public class DynamoDBTest {

    private final static Duration timeout = Duration.create("30 seconds");

    private final static DynamoDB ddb = new DynamoDB(DDBRegion.EU_WEST_1);

    private final static ProvisionedThroughput throughput = new ProvisionedThroughput(10L, 10L);

    private static Map<String, AttributeValue> NTESLA = new HashMap<String, AttributeValue>();
    static {
        NTESLA.put("id", AttributeValue.createS("ntesla"));
        NTESLA.put("firstName", AttributeValue.createS("Nikola"));
        NTESLA.put("lastName", AttributeValue.createS("Tesla"));
        NTESLA.put("awesomeLevel", AttributeValue.createN(1000L));
    }

    private static Map<String, AttributeValue> TEDISON = new HashMap<String, AttributeValue>();
    static {
        TEDISON.put("id", AttributeValue.createS("tedison"));
        TEDISON.put("firstName", AttributeValue.createS("Thomas"));
        TEDISON.put("lastName", AttributeValue.createS("Edison"));
        TEDISON.put("awesomeLevel", AttributeValue.createN(800L));
    }

    private <T> T get(Future<T> f) throws Exception {
        return Await.result(f, timeout);
    }

    private void waitUntilReady(String tableName) throws Exception {
        boolean ready = false;
        while (!ready) {
            Thread.sleep(2000);
            TableStatus status = get(ddb.describeTable(tableName)).body().status();
            if (status != TableStatus.Creating && status != TableStatus.Updating) ready = true;
        }
        Thread.sleep(2000);
    }

    @Test
    public void createAndDeleteTable() throws Exception {
        String table = "java-create-table";
        PrimaryKey key = new PrimaryKey(new KeySchemaElement("login", AttributeType.StringType));
        SimpleResult<TableDescription> result = get(ddb.createTable(table, key, throughput));
        assertTrue(result.toString(), result.isSuccess());
        waitUntilReady(table);
        SimpleResult<Object> result2 = get(ddb.deleteTable(table));
        assertTrue(result2.toString(), result2.isSuccess());
    }

    @Test
    public void putAndGetItems() throws Exception {
        String table = "java-putget";
        PrimaryKey key = new PrimaryKey(new KeySchemaElement("id", AttributeType.StringType));
        SimpleResult<TableDescription> result = get(ddb.createTable(table, key, throughput));
        assertTrue(result.toString(), result.isSuccess());
        waitUntilReady(table);

        SimpleResult<ItemResponse> putResult = get(ddb.putItem(table, NTESLA));
        assertTrue(putResult.toString(), putResult.isSuccess());
        SimpleResult<ItemResponse> getResult = get(ddb.getItem(table, KeyValue.hashKey("ntesla"), true));
        assertTrue(getResult.toString(), getResult.isSuccess());
        assertEquals("First name should be Nikola, but we got " + getResult.toString(),
                getResult.body().get("firstName").getS(), "Nikola");

        waitUntilReady(table);
        SimpleResult<Object> result2 = get(ddb.deleteTable(table));
        assertTrue(result2.toString(), result2.isSuccess());
    }

    @Test
    public void query() throws Exception {
        String table = "java-query";
        PrimaryKey key = new PrimaryKey(
                new KeySchemaElement("id", AttributeType.StringType),
                new KeySchemaElement("awesomeLevel", AttributeType.NumberType)
                );
        SimpleResult<TableDescription> result = get(ddb.createTable(table, key, throughput));
        assertTrue(result.toString(), result.isSuccess());
        waitUntilReady(table);

        SimpleResult<ItemResponse> putResult = get(ddb.putItem(table, NTESLA));
        assertTrue(putResult.toString(), putResult.isSuccess());

        Query query = new Query(table).withConsistentRead(true).withHashKeyValue(AttributeValue.createS("ntesla"));
        SimpleResult<QueryResponse> queryResult = get(ddb.query(query));
        assertTrue(queryResult.toString(), queryResult.isSuccess());
        assertTrue("Didn't found any result, should have found ntesla", queryResult.body().count() > 0);
        assertNotNull("Couldn't find attribute firstName", queryResult.body().itemAt(0).get("firstName"));
        assertTrue("firstName should be Nikola", queryResult.body().itemAt(0).get("firstName").getS().equals("Nikola"));

        waitUntilReady(table);
        SimpleResult<Object> result2 = get(ddb.deleteTable(table));
        assertTrue(result2.toString(), result2.isSuccess());
    }

    @Test
    public void batch() throws Exception {
        String table = "java-batch";
        PrimaryKey key = new PrimaryKey(
                new KeySchemaElement("id", AttributeType.StringType),
                new KeySchemaElement("awesomeLevel", AttributeType.NumberType)
                );
        SimpleResult<TableDescription> result = get(ddb.createTable(table, key, throughput));
        assertTrue(result.toString(), result.isSuccess());

        waitUntilReady(table);
        List<WriteRequest> requests = new ArrayList<WriteRequest>();
        requests.add(WriteRequest.put(NTESLA));
        requests.add(WriteRequest.put(TEDISON));
        SimpleResult<BatchWriteResponse> writeResult = get(ddb.batchWriteItem(table, requests));
        assertTrue(writeResult.toString(), writeResult.isSuccess());

        List<GetRequest> grequests = new ArrayList<GetRequest>();
        SimpleResult<BatchGetResponse> getResult = get(ddb.batchGetItem(grequests));
        assertTrue(getResult.toString(), getResult.isSuccess());
//        assertEquals("First name should be Thomas, but we got " + getResult.toString(),
//                getResult.body().get("firstName").getS(), "Thomas");

        waitUntilReady(table);
        SimpleResult<Object> result2 = get(ddb.deleteTable(table));
        assertTrue(result2.toString(), result2.isSuccess());
    }

}*/