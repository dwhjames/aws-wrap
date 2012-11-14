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

