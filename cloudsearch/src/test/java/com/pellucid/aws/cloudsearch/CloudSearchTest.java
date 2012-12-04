package com.pellucid.aws.dynamodb;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import com.pellucid.aws.results.SimpleResult;

import com.pellucid.aws.cloudsearch.CloudSearch;
import com.pellucid.aws.cloudsearch.models.*;

public class CloudSearchTest {

    private final static Duration timeout = Duration.create("30 seconds");
    private final static CloudSearch search = new CloudSearch();
    private final static Domain domain = new Domain("imdb-movies", "5d3sfdtvri2lghw27klaho756y");

    //private final static Parser movieParser = new P

    private <T> T get(Future<T> f) throws Exception {
        return Await.result(f, timeout);
    }

    @Test
    public void query() throws Exception {
      Search s = new Search(domain);
      //search.search(domain, parser);
      assertTrue("youpi", true);
    }

}
