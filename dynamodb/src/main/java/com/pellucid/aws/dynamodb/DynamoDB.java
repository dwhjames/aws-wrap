package com.pellucid.aws.dynamodb;

import java.util.List;

import com.pellucid.aws.internal.AWSJavaConversions;
import com.pellucid.aws.results.SimpleResult;

import play.libs.F;
import play.libs.Scala;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import com.pellucid.aws.dynamodb.models.*;

public class DynamoDB {

    private final aws.dynamodb.DDBRegion scalaRegion;

    public DynamoDB(DDBRegion region) {
        this.scalaRegion = DynamoDB.scalaRegion(region);
    }

    public F.Promise<SimpleResult<List<String>>> listTables() {
        return this.listTables(null, null);
    }

    public F.Promise<SimpleResult<List<String>>> listTables(String exclusiveStartTableName) {
        return this.listTables(null, exclusiveStartTableName);
    }

    public F.Promise<SimpleResult<List<String>>> listTables(Integer limit) {
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
    public F.Promise<SimpleResult<List<String>>> listTables(Integer limit, String exclusiveStartTableName) {
        return AWSJavaConversions.toSimpleResultPromise(aws.dynamodb.DynamoDB.listTables(Scala.Option((Object)limit), Scala.Option(exclusiveStartTableName), scalaRegion),
                new F.Function<Seq<String>, List<String>>() {
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
/*    public F.Promise<SimpleResult<TableDescription>> createTable(String tableName, PrimaryKey keySchema, ProvisionedThroughput provisionedThroughput) {
        return AWSJavaConversions.toSimpleResultPromise(aws.dynamodb.DynamoDB.createTable(tableName, keySchema.toScala(), provisionedThroughput.toScala(), scalaRegion),
                new F.Function<Seq<String>, List<String>>() {
            @Override public List<String> apply(Seq<String> tableNames) {
                return JavaConversions.seqAsJavaList(tableNames);
            }
        });
    }*/

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

}
