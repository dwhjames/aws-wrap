package com.pellucid.aws.cloudsearch;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import org.codehaus.jackson.JsonNode;

import org.junit.Test;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import play.libs.WS.Response;

import com.pellucid.aws.core.parsers.*;

import com.pellucid.aws.results.Result;
import com.pellucid.aws.cloudsearch.CloudSearch;
import com.pellucid.aws.cloudsearch.models.*;

// HUMF
import aws.core.*;
import aws.cloudsearch.CloudSearchMetadata;

public class CloudSearchTest {

    private final static Duration timeout = Duration.create("30 seconds");
    private final static CloudSearch cloudSearch = new CloudSearch();
    private final static Domain domain = new Domain("imdb-movies", "5d3sfdtvri2lghw27klaho756y");

    public static class Movie {
      public String id;
      public List<String> titles;
      public Movie(String id, List<String> titles){
        this.id = id;
        this.titles = titles;
      }

      public String toString() {
        return String.format("Movie(%s, %s)", id, titles);
      }
    }

    private final static Parser movieParser = new BaseParser<List<Movie>>() {
      @Override
      public ParseResult<List<Movie>> apply(Response resp) {
        List<Movie> ms = new ArrayList<Movie>();
        System.out.println();
        for(JsonNode movie : resp.asJson().findPath("hit")){
          JsonNode id = movie.findPath("id");
          List<String> titles = new ArrayList<String>();
          for(JsonNode title : movie.findPath("title")){
            titles.add(title.asText());
          }
          ms.add(new Movie(id.asText(), titles));
        }
        return new Success(ms);
      }
    };

    private <T> T get(Future<T> f) throws Exception {
      return Await.result(f, timeout);
    }

    @Test
    public void query() throws Exception {
      Search s = new Search(domain)
        .withReturnFields("title")
        .withQuery("star wars");
      scala.concurrent.Future<Result<CloudSearchMetadata, List<Movie>>> eventuallyResult = cloudSearch.search(s, movieParser);
      Result<CloudSearchMetadata, List<Movie>> result = get(eventuallyResult);
      assertTrue("request failed", result.isSuccess());
      assertFalse("empty result", result.body().isEmpty());
    }

}
