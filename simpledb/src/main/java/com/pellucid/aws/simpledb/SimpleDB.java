package com.pellucid.aws.simpledb;

import java.util.List;
import java.util.ArrayList;

import play.libs.F;
import play.libs.Scala;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.concurrent.Future;
import scala.runtime.BoxedUnit;

import com.pellucid.aws.internal.AWSJavaConversions;
import com.pellucid.aws.internal.MetadataConvert;
import com.pellucid.aws.results.Result;

public class SimpleDB {

    private final aws.simpledb.SDBRegion scalaRegion;

    public SimpleDB(SDBRegion region) {
        this.scalaRegion = SimpleDB.scalaRegion(region);
    }

    /**
     * Creates a new domain. The domain name must be unique among the domains associated with the Access Key ID used.
     * The CreateDomain operation might take 10 or more seconds to complete.
     * CreateDomain is an idempotent operation; running it multiple times using the same domain name will not result
     * in an error response.
     *
     * You can create up to 250 domains per account.
     * If you require additional domains, go to http://aws.amazon.com/contact-us/simpledb-limit-request/.
     *
     * @param domainName
     */
    public F.Promise<Result<SimpleDBMeta, Object>> createDomain(String domainName) {
        return convertEmptyResult(aws.simpledb.SimpleDB.createDomain(domainName, scalaRegion));
    }

    /**
     * Deletes the given domain. Any items (and their attributes) in the domain are deleted as well.
     * The DeleteDomain operation might take 10 or more seconds to complete.
     *
     * Running DeleteDomain on a domain that does not exist or running the function multiple times
     * using the same domain name will not result in an error response.
     *
     * @param domainName
     */
    public F.Promise<Result<SimpleDBMeta, Object>> deleteDomain(String domainName) {
        return convertEmptyResult(aws.simpledb.SimpleDB.deleteDomain(domainName, scalaRegion));
    }

    public F.Promise<Result<SimpleDBMeta, List<String>>> listDomains() {
        return listDomains(100, null);
    }

    public F.Promise<Result<SimpleDBMeta, List<String>>> listDomains(Integer maxNumberOfDomains) {
        return listDomains(maxNumberOfDomains, null);
    }

    public F.Promise<Result<SimpleDBMeta, List<String>>> listDomains(String nextToken) {
        return listDomains(100, nextToken);
    }

    /**
     * Lists all domains associated with the Access Key ID.
     * It returns domain names up to the limit set by MaxNumberOfDomains. A NextToken is returned if
     * there are more than MaxNumberOfDomains domains. Calling ListDomains successive times with the NextToken returns up to MaxNumberOfDomains more domain names each time.
     *
     * @param maxNumberOfDomains limit the response to a size
     * @param nextToken: optionally provide this value you got from a previous request to get the next page
     */
    public F.Promise<Result<SimpleDBMeta, List<String>>> listDomains(Integer maxNumberOfDomains, String nextToken) {
        return AWSJavaConversions.toResultPromise(aws.simpledb.SimpleDB.listDomains(maxNumberOfDomains, Scala.Option(nextToken), scalaRegion),
                new MetadataConvert(),
                new F.Function<Seq<aws.simpledb.SDBDomain>, List<String>>() {
            @Override public List<String> apply(Seq<aws.simpledb.SDBDomain> domains) {
                return domainSeqToStringList(domains);
            }
        });
    }

    public F.Promise<Result<SimpleDBMeta, Object>> putAttributes(
            String domainName,
            String itemName,
            List<SDBAttribute> attributes) {
        return this.putAttributes(domainName, itemName, attributes, new ArrayList<SDBExpected>());
    }

    /**
     * Creates or replaces attributes in an item.
     *
     * Using PutAttributes to replace attribute values that do not exist will not result in an error response.
     *
     * When using eventually consistent reads, a GetAttributes or Select request (read) immediately after a DeleteAttributes or PutAttributes request (write) might not return the updated data. A consistent read always reflects all writes that received a successful response prior to the read. For more information, see Consistency.
     *
     * @param domainName
     * @param itemName the item to update. May not be empty.
     * @param attributes the attributes to create or update
     * @param expected if defined, perform the expected conditional check on one attribute. If expected.value is None,
     *                 will check that the attribute exists. If expected.value is defined, will check that the attribute
     *                 exists and has the specified value.
     */
    public F.Promise<Result<SimpleDBMeta, Object>> putAttributes(
            String domainName,
            String itemName,
            List<SDBAttribute> attributes,
            List<SDBExpected> expected) {
        Seq<aws.simpledb.SDBAttribute> sAttributes = SDBAttribute.listAsScalaSeq(attributes);
        Seq<aws.simpledb.SDBExpected> sExpected = SDBExpected.listAsScalaSeq(expected);
        return convertEmptyResult(aws.simpledb.SimpleDB.putAttributes(domainName, itemName, sAttributes, sExpected, scalaRegion));
    }

    public F.Promise<Result<SimpleDBMeta, Object>> deleteAttributes(
            String domainName,
            String itemName) {
        return this.deleteAttributes(domainName, itemName, new ArrayList<SDBAttribute>());
    }

    public F.Promise<Result<SimpleDBMeta, Object>> deleteAttributes(
            String domainName,
            String itemName,
            List<SDBAttribute> attributes) {
        return this.deleteAttributes(domainName, itemName, attributes, new ArrayList<SDBExpected>());
    }

    /**
     * Deletes one or more attributes associated with the item. If all attributes of an item are deleted, the item is deleted.
     *
     * If you specify DeleteAttributes without attributes or values, all the attributes for the item are deleted.
     *
     * Unless you specify conditions, the DeleteAttributes is an idempotent operation; running it multiple times on the same item or attribute
     * does not result in an error response.
     *
     * Conditional deletes are useful for only deleting items and attributes if specific conditions are met. If the conditions are met, Amazon SimpleDB performs the delete. Otherwise, the data is not deleted.
     *
     * When using eventually consistent reads, a GetAttributes or Select request (read) immediately after a DeleteAttributes or PutAttributes request (write) might not return the updated data. A consistent read always reflects all writes that received a successful response prior to the read.
     * For more information, see [[http://docs.amazonwebservices.com/AmazonSimpleDB/latest/DeveloperGuide/ConsistencySummary.html Consistency]].
     *
     * @param domainName
     * @param itemName the item to update. May not be empty.
     * @param attributes the list of attributes to delete
     * @param expected if defined, perform the expected conditional check on one attribute. If expected.value is None,
     *                 will check that the attribute exists. If expected.value is defined, will check that the attribute
     *                 exists and has the specified value.
     *
     */
    public F.Promise<Result<SimpleDBMeta, Object>> deleteAttributes(
            String domainName,
            String itemName,
            List<SDBAttribute> attributes,
            List<SDBExpected> expected) {
        Seq<aws.simpledb.SDBAttribute> sAttributes = SDBAttribute.listAsScalaSeq(attributes);
        Seq<aws.simpledb.SDBExpected> sExpected = SDBExpected.listAsScalaSeq(expected);
        return convertEmptyResult(aws.simpledb.SimpleDB.deleteAttributes(domainName, itemName, sAttributes, sExpected, scalaRegion));
    }

    private static List<String> domainSeqToStringList(Seq<aws.simpledb.SDBDomain> domains) {
        List<String> result = new ArrayList<String>();
        for (aws.simpledb.SDBDomain domain: JavaConversions.seqAsJavaList(domains)) {
            result.add(domain.name());
        }
        return result;
    }

    private static F.Promise<Result<SimpleDBMeta, Object>> convertEmptyResult(Future<aws.core.Result<aws.simpledb.SimpleDBMeta, BoxedUnit>> scalaResult) {
        return AWSJavaConversions.toResultPromise(scalaResult, new MetadataConvert(), new F.Function<BoxedUnit, Object>() {
            @Override public Object apply(BoxedUnit unit) throws Throwable {
                return null;
            }
        });
    }

    private static aws.simpledb.SDBRegion scalaRegion(SDBRegion region) {
        switch (region) {
        case US_EAST_1: return aws.simpledb.SDBRegion$.MODULE$.US_EAST_1();
        case US_WEST_1: return aws.simpledb.SDBRegion$.MODULE$.US_WEST_1();
        case US_WEST_2: return aws.simpledb.SDBRegion$.MODULE$.US_WEST_2();
        case EU_WEST_1: return aws.simpledb.SDBRegion$.MODULE$.EU_WEST_1();
        case ASIA_SOUTHEAST_1: return aws.simpledb.SDBRegion$.MODULE$.ASIA_SOUTHEAST_1();
        case ASIA_NORTHEAST_1: return aws.simpledb.SDBRegion$.MODULE$.ASIA_NORTHEAST_1();
        case SA_EAST_1: return aws.simpledb.SDBRegion$.MODULE$.SA_EAST_1();
        }
        return aws.simpledb.SDBRegion$.MODULE$.DEFAULT();
    }

}

