package com.pellucid.aws.dynamodb;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import com.pellucid.aws.dynamodb.models.KeySchemaElement;
import com.pellucid.aws.dynamodb.models.KeySchemaElement.AttributeType;
import com.pellucid.aws.dynamodb.models.PrimaryKey;
import com.pellucid.aws.dynamodb.models.ProvisionedThroughput;
import com.pellucid.aws.dynamodb.models.TableDescription;
import com.pellucid.aws.dynamodb.models.TableStatus;
import com.pellucid.aws.results.SimpleResult;

public class DynamoDBTest {

    private final static Duration timeout = Duration.create("30 seconds");

    private final static DynamoDB ddb = new DynamoDB(DDBRegion.EU_WEST_1);

    private final static ProvisionedThroughput throughput = new ProvisionedThroughput(10L, 10L);

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
        PrimaryKey key = new PrimaryKey(new KeySchemaElement("login", AttributeType.StringType));
        SimpleResult<TableDescription> result = get(ddb.createTable("java-create-table", key, throughput));
        assertTrue(result.toString(), result.isSuccess());
        waitUntilReady("java-create-table");
        SimpleResult<Object> result2 = get(ddb.deleteTable("java-create-table"));
        assertTrue(result2.toString(), result2.isSuccess());
    }

}
