package com.pellucid.aws.dynamodb;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import com.pellucid.aws.dynamodb.models.KeySchemaElement;
import com.pellucid.aws.dynamodb.models.KeySchemaElement.AttributeType;
import com.pellucid.aws.dynamodb.models.PrimaryKey;
import com.pellucid.aws.dynamodb.models.ProvisionedThroughput;
import com.pellucid.aws.dynamodb.models.TableDescription;
import com.pellucid.aws.results.SimpleResult;

public class DynamoDBTest {

    private final static Duration timeout = Duration.create("30 seconds");

    private final static DynamoDB ddb = new DynamoDB(DDBRegion.EU_WEST_1);

    private final static ProvisionedThroughput throughput = new ProvisionedThroughput(10L, 10L);

    @Test
    public void createAndDeleteTable() throws Exception {
        PrimaryKey key = new PrimaryKey(new KeySchemaElement("login", AttributeType.StringType));
        SimpleResult<TableDescription> result = Await.result(ddb.createTable("java-create-table", key, throughput), timeout);
        assertTrue(result.toString(), result.isSuccess());
        SimpleResult<Object> result2 = Await.result(ddb.deleteTable("java-create-table"), timeout);
        assertTrue(result2.toString(), result2.isSuccess());
    }

}
