package com.pellucid.aws.simpledb;

import scala.runtime.BoxedUnit;

import com.pellucid.aws.AWSRegion;
import com.pellucid.aws.V2Requester;
import com.pellucid.aws.results.Result;
import com.pellucid.aws.internal.JavaConversions;
import com.pellucid.aws.internal.MetadataConvert;

import scala.concurrent.Future;
import play.libs.F;

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
        Future<aws.core.Result<aws.simpledb.SimpleDBMeta, scala.runtime.BoxedUnit>> scalaResult = aws.simpledb.SimpleDB.createDomain(domainName, scalaRegion);
        return JavaConversions.toResultPromise(scalaResult, new MetadataConvert(), new F.Function<BoxedUnit, Object>() {
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

