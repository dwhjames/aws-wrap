package com.pellucid.aws.dynamodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import play.libs.Scala;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.concurrent.Future;
import scala.runtime.BoxedUnit;
import akka.dispatch.Mapper;

import com.pellucid.aws.internal.*;
import com.pellucid.aws.results.SimpleResult;

import com.pellucid.aws.dynamodb.models.*;

public class DynamoDB {

    private final aws.dynamodb.DDBRegion scalaRegion;

    public DynamoDB(DDBRegion region) {
        this.scalaRegion = DynamoDB.scalaRegion(region);
    }

    public Future<SimpleResult<List<String>>> listTables() {
        return this.listTables(null, null);
    }

    public Future<SimpleResult<List<String>>> listTables(String exclusiveStartTableName) {
        return this.listTables(null, exclusiveStartTableName);
    }

    public Future<SimpleResult<List<String>>> listTables(Integer limit) {
        return this.listTables(limit, null);
    }

    /**
     * Returns a sequence of all the tables names associated with the current account and endpoint.
     *
     * The ListTables operation returns all of the table names associated with the account making the request, for the endpoint that receives the request.
     *
     * @param limit A number of maximum table names to return
     * @param exclusiveStartTableName The name of the table that starts the list. If you already ran a ListTables operation and received an LastEvaluatedTableName value in the response, use that value here to continue the list.
     */
    public Future<SimpleResult<List<String>>> listTables(Integer limit, String exclusiveStartTableName) {
        return AWSJavaConversions.toJavaSimpleResult(aws.dynamodb.DynamoDB.listTables(Scala.Option((Object)limit), Scala.Option(exclusiveStartTableName), scalaRegion, aws.core.AWS.defaultExecutionContext()),
                new Mapper<Seq<String>, List<String>>() {
            @Override public List<String> apply(Seq<String> tableNames) {
                return JavaConversions.seqAsJavaList(tableNames);
            }
        });
    }

    /**
     * The CreateTable operation adds a new table to your account. The table name must be unique among those associated with the AWS Account issuing the request,
     * and the AWS region that receives the request (such as dynamodb.us-east-1.amazonaws.com).
     *
     * The CreateTable operation triggers an asynchronous workflow to begin creating the table. Amazon DynamoDB immediately returns the state of the table ([[Status.CREATING CREATING]])
     * until the table is in the [[Status.ACTIVE ACTIVE]] state. Once the table is in the [[Status.ACTIVE ACTIVE]] state, you can perform data plane operations.
     * Use [[describeTable]] to check the status of the table.
     *
     * @param tableName The name of the table to create.
     *                  Allowed characters are a-z, A-Z, 0-9, '_' (underscore), '-' (dash), and '.' (dot).
     *                  Names can be between 3 and 255 characters long.
     * @param keySchema the primary key structure for the table. See [[PrimaryKey]] for more information.
     */
    public Future<SimpleResult<TableDescription>> createTable(String tableName,
            PrimaryKey keySchema,
            ProvisionedThroughput provisionedThroughput) {
        return AWSJavaConversions.toJavaSimpleResult(aws.dynamodb.DynamoDB.createTable(tableName, keySchema.toScala(), provisionedThroughput.toScala(), scalaRegion, aws.core.AWS.defaultExecutionContext()),
                new Mapper<aws.dynamodb.TableDescription, TableDescription>() {
            @Override public TableDescription apply(aws.dynamodb.TableDescription tableDesc) {
                return TableDescription.fromScala(tableDesc);
            }
        });
    }

    /**
     * Updates the provisioned throughput for the given table. Setting the throughput for a table
     * helps you manage performance and is part of the provisioned throughput feature of Amazon DynamoDB.
     * For more information, see [[http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/WorkingWithDDTables.html#ProvisionedThroughput Specifying Read and Write Requirements (Provisioned Throughput)]].
     *
     * The provisioned throughput values can be upgraded or downgraded based on the maximums and minimums listed in Limits in Amazon DynamoDB.
     *
     * The table must be in the [[Status.ACTIVE ACTIVE]] state for this operation to succeed.
     * UpdateTable is an asynchronous operation; while executing the operation, the table is in the [[Status.UPDATING UPDATING]] state.
     * While the table is in the [[Status.UPDATING UPDATING]] state, the table still has the provisioned throughput from before the call.
     * The new provisioned throughput setting is in effect only when the table returns to the [[Status.ACTIVE ACTIVE]] state after the UpdateTable operation.
     *
     * @param tableName
     */
    public Future<SimpleResult<TableDescription>> updateTable(String tableName,
            ProvisionedThroughput provisionedThroughput) {
        return AWSJavaConversions.toJavaSimpleResult(
                aws.dynamodb.DynamoDB.updateTable(tableName,
                        provisionedThroughput.toScala(),
                        scalaRegion,
                        aws.core.AWS.defaultExecutionContext()),
                        new Mapper<aws.dynamodb.TableDescription, TableDescription>() {
                    @Override public TableDescription apply(aws.dynamodb.TableDescription tableDesc) {
                        return TableDescription.fromScala(tableDesc);
                    }
                });
    }

    /**
     * The DeleteTable operation deletes a table and all of its items.
     * After a DeleteTable request, the specified table is in the [[Status.DELETING DELETING]] state until Amazon DynamoDB completes
     * the deletion.
     *
     *  - If the table is in the [[Status.ACTIVE ACTIVE]] state, you can delete it.
     *  - If a table is in [[Status.CREATING CREATING]] or [[Status.UPDATING UPDATING]] states,
     * then Amazon DynamoDB returns a ResourceInUseException error.
     *  - If the specified table does not exist, Amazon DynamoDB returns a ResourceNotFoundException.
     *  - If table is already in the [[Status.DELETING DELETING]] state, no error is returned.
     *
     * Amazon DynamoDB might continue to accept data plane operation requests, such as [[getItem]] and [[putItem]],
     * on a table in the DELETING state until the table deletion is complete.
     *
     * @param tableName
     */
    public Future<SimpleResult<Object>> deleteTable(String tableName) {
        return AWSJavaConversions.toJavaSimpleResult(
                aws.dynamodb.DynamoDB.deleteTable(tableName, scalaRegion, aws.core.AWS.defaultExecutionContext()),
                new Mapper<BoxedUnit, Object>() {
                    @Override public Object apply(BoxedUnit unit) {
                        return null;
                    }
                }
                );
    }

    /**
     * Returns information about the table, including the current status of the table,
     * the primary key schema and when the table was created.
     *
     * DescribeTable results are eventually consistent.
     * If you use DescribeTable too early in the process of creating a table, Amazon DynamoDB returns a ResourceNotFoundException.
     * If you use DescribeTable too early in the process of updating a table, the new values might not be immediately available.
     *
     * @param tableName
     */
    public Future<SimpleResult<TableDescription>> describeTable(String tableName) {
        return AWSJavaConversions.toJavaSimpleResult(
                aws.dynamodb.DynamoDB.describeTable(tableName, scalaRegion, aws.core.AWS.defaultExecutionContext()),
                new Mapper<aws.dynamodb.TableDescription, TableDescription>() {
                    @Override public TableDescription apply(aws.dynamodb.TableDescription tableDesc) {
                        return TableDescription.fromScala(tableDesc);
                    }
                }
                );
    }

    public Future<SimpleResult<ItemResponse>> putItem(String tableName, Map<String, AttributeValue> item) {
        return putItem(tableName, item, new HashMap<String, Expected>(), ReturnValues.NONE);
    }

    public Future<SimpleResult<ItemResponse>> putItem(String tableName,
            Map<String, AttributeValue> item,
            ReturnValues returnValues) {
        return putItem(tableName, item, new HashMap<String, Expected>(), returnValues);
    }

    public Future<SimpleResult<ItemResponse>> putItem(String tableName,
            Map<String, AttributeValue> item,
            Map<String, Expected> expected) {
        return putItem(tableName, item, expected, ReturnValues.NONE);
    }

    /**
     * Creates a new item, or replaces an old item with a new item (including all the attributes).
     * If an item already exists in the specified table with the same primary key, the new item completely replaces the existing item. You can perform a conditional put (insert a new item if one with the specified primary key doesn't exist), or replace an existing item if it has certain attribute values.
     *
     * @param tableName
     * @param item The [[Item]] to put. Must include the primary key values that define the item.
     * Other attribute name-value pairs can be provided for the item. For more information about primary keys, see [[PrimaryKey]].
     */
    public Future<SimpleResult<ItemResponse>> putItem(String tableName,
            Map<String, AttributeValue> item,
            Map<String, Expected> expected,
            ReturnValues returnValues) {
        aws.dynamodb.Item sItem = scalaItemFromMap(item);
        Map<String, aws.dynamodb.Expected> sExpected = new HashMap<String, aws.dynamodb.Expected>();
        for (String key: expected.keySet()) {
            sExpected.put(key, expected.get(key).toScala());
        }
        scala.Enumeration.Value rv;
        if (returnValues == ReturnValues.ALL_OLD) {
            rv = aws.dynamodb.ReturnValues$.MODULE$.ALL_OLD();
        } else {
            rv = aws.dynamodb.ReturnValues$.MODULE$.NONE();
        }
        return itemResponseConvert(
                aws.dynamodb.DynamoDB.putItem(
                        tableName,
                        sItem,
                        AWSJavaConversions.toScalaMap(sExpected),
                        rv,
                        scalaRegion,
                        aws.core.AWS.defaultExecutionContext())
                );
    }

    public Future<SimpleResult<ItemResponse>> getItem(String tableName, KeyValue key) {
        return getItem(tableName, key, new ArrayList<String>());
    }

    public Future<SimpleResult<ItemResponse>> getItem(String tableName, KeyValue key, boolean consistentRead) {
        return getItem(tableName, key, new ArrayList<String>(), consistentRead);
    }

    public Future<SimpleResult<ItemResponse>> getItem(String tableName, KeyValue key, List<String> attributesToGet) {
        return getItem(tableName, key, attributesToGet, false);
    }

    /**
     * The GetItem operation returns a set of Attributes for an item that matches the primary key.
     *
     * The GetItem operation provides an eventually consistent read by default.
     * If eventually consistent reads are not acceptable for your application, use ConsistentRead.
     * Although this operation might take longer than a standard read, it always returns the last updated value.
     * For more information, see [[http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/APISummary.html#DataReadConsistency Data Read and Consistency Considerations]].
     */
    public Future<SimpleResult<ItemResponse>> getItem(
            String tableName,
            KeyValue key,
            List<String> attributesToGet,
            Boolean consistentRead) {
        return itemResponseConvert(
                aws.dynamodb.DynamoDB.getItem(
                        tableName,
                        key.toScala(),
                        JavaConversions.asScalaIterable(attributesToGet).toSeq(),
                        consistentRead,
                        scalaRegion,
                        aws.core.AWS.defaultExecutionContext())
                );
    }

    public Future<SimpleResult<QueryResponse>> query(Query query) {
        return AWSJavaConversions.toJavaSimpleResult(
                aws.dynamodb.DynamoDB.query(query.toScala(), scalaRegion, aws.core.AWS.defaultExecutionContext()),
                new Mapper<aws.dynamodb.QueryResponse, QueryResponse>() {
                    @Override public QueryResponse apply(aws.dynamodb.QueryResponse result) {
                        return QueryResponse.fromScala(result);
                    }
                });
    }

    public Future<SimpleResult<QueryResponse>> scan(
            String tableName,
            List<String> attributesToGet,
            Long limit,
            boolean count,
            KeyCondition scanFilter,
            PrimaryKey exclusiveStartKey) {
        return AWSJavaConversions.toJavaSimpleResult(
                aws.dynamodb.DynamoDB.scan(
                        tableName,
                        JavaConversions.asScalaIterable(attributesToGet).toSeq(),
                        Scala.Option((Object)limit),
                        count,
                        Scala.Option(scanFilter == null ? null : scanFilter.toScala()),
                        Scala.Option(exclusiveStartKey == null ? null : exclusiveStartKey.toScala()),
                        scalaRegion, aws.core.AWS.defaultExecutionContext()),
                        new Mapper<aws.dynamodb.QueryResponse, QueryResponse>() {
                    @Override public QueryResponse apply(aws.dynamodb.QueryResponse result) {
                        return QueryResponse.fromScala(result);
                    }
                });
    }

    public Future<SimpleResult<BatchWriteResponse>> batchWriteItem(String tableName, List<WriteRequest> writeRequests) {
        Map<String, List<WriteRequest>> requestItems = new HashMap<String, List<WriteRequest>>();
        requestItems.put(tableName, writeRequests);
        return batchWriteItem(requestItems);
    }

    public Future<SimpleResult<BatchWriteResponse>> batchWriteItem(Map<String, List<WriteRequest>> requestItems) {
        return AWSJavaConversions.toJavaSimpleResult(
                aws.dynamodb.DynamoDB.batchWriteItem(
                        WriteRequest.requestMapToScala(requestItems),
                        scalaRegion, aws.core.AWS.defaultExecutionContext()),
                        new Mapper<aws.dynamodb.BatchWriteResponse, BatchWriteResponse>() {
                    @Override public BatchWriteResponse apply(aws.dynamodb.BatchWriteResponse result) {
                        return BatchWriteResponse.fromScala(result);
                    }
                });
    }

    public Future<SimpleResult<BatchGetResponse>> batchGetItem(List<GetRequest> requestItems) {
        return AWSJavaConversions.toJavaSimpleResult(
                aws.dynamodb.DynamoDB.batchGetItem(
                        AWSJavaConversions.toSeq(Lists.map(requestItems, new Mapper<GetRequest, aws.dynamodb.GetRequest>(){
                            @Override public aws.dynamodb.GetRequest apply(GetRequest request) {
                                return request.toScala();
                            }
                        })),
                        scalaRegion, aws.core.AWS.defaultExecutionContext()),
                        new Mapper<aws.dynamodb.BatchGetResponse, BatchGetResponse>() {
                    @Override public BatchGetResponse apply(aws.dynamodb.BatchGetResponse result) {
                        System.out.println("Response: " + result);
                        return BatchGetResponse.fromScala(result);
                    }
                });
    }

    private static <MS extends aws.core.Metadata> Future<SimpleResult<ItemResponse>> itemResponseConvert(Future<aws.core.Result<MS, aws.dynamodb.ItemResponse>> scalaResponse) {
        return AWSJavaConversions.toJavaSimpleResult(
                scalaResponse,
                new Mapper<aws.dynamodb.ItemResponse, ItemResponse>() {
                    @Override public ItemResponse apply(aws.dynamodb.ItemResponse resp) {
                        return ItemResponse.fromScala(resp);
                    }
                }
                );
    }

    private static aws.dynamodb.Item scalaItemFromMap(Map<String, AttributeValue> item) {
        List<scala.Tuple2<String, aws.dynamodb.DDBAttribute>> sAttrs = new ArrayList<scala.Tuple2<String, aws.dynamodb.DDBAttribute>>();
        for (String key: item.keySet()) {
            sAttrs.add(new scala.Tuple2<String, aws.dynamodb.DDBAttribute>(key, item.get(key).toScala()));
        }
        return new aws.dynamodb.Item(JavaConversions.asScalaIterable(sAttrs).toSeq());
    }

    private static aws.dynamodb.DDBRegion scalaRegion(DDBRegion region) {
        switch (region) {
        case US_EAST_1: return aws.dynamodb.DDBRegion$.MODULE$.US_EAST_1();
        case US_WEST_1: return aws.dynamodb.DDBRegion$.MODULE$.US_WEST_1();
        case US_WEST_2: return aws.dynamodb.DDBRegion$.MODULE$.US_WEST_2();
        case EU_WEST_1: return aws.dynamodb.DDBRegion$.MODULE$.EU_WEST_1();
        case ASIA_SOUTHEAST_1: return aws.dynamodb.DDBRegion$.MODULE$.ASIA_SOUTHEAST_1();
        case ASIA_NORTHEAST_1: return aws.dynamodb.DDBRegion$.MODULE$.ASIA_NORTHEAST_1();
        }
        return aws.dynamodb.DDBRegion$.MODULE$.DEFAULT();
    }

    public enum ReturnValues {
        NONE, ALL_OLD
    }

}
