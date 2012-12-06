package com.pellucid.aws.cloudsearch;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

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

import static com.pellucid.aws.cloudsearch.models.MatchExpression.*;

// TODO: HUMF
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

      @Override
      public boolean equals(Object m) {
        if(m == null)
          return false;
        if(!(m instanceof Movie))
          return false;
        return this.id.equals(((Movie)m).id);
      }
    }

    private Search base = new Search(domain).withReturnFields("title");

    private final static Parser<List<Movie>> movieParser = new BaseParser<List<Movie>>() {
      @Override
      public ParseResult<List<Movie>> apply(Response resp) {
        List<Movie> ms = new ArrayList<Movie>();

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
    public void stringQuery() throws Exception {
      Search s = base.withQuery("star wars");
      Result<CloudSearchMetadata, List<Movie>> result = get(cloudSearch.search(s, movieParser));
      assertTrue("request failed", result.isSuccess());
      assertFalse("empty result", result.body().isEmpty());
    }

    @Test
    public void matchExpressionAndFilter() throws Exception {
      MatchExpression ex = field("title", "star wars")
        .and(filterValue("year", 2008));

      Search s = base.withMatchExpression(ex);
      Result<CloudSearchMetadata, List<Movie>> result = get(cloudSearch.search(s, movieParser));
      assertTrue("request failed", result.isSuccess());
      assertFalse("empty result", result.body().isEmpty());
    }

    @Test
    public void matchExpressionAndFilterRange() throws Exception {
      MatchExpression ex = field("title", "star wars")
        .and(filterRange("year", 2000, 2012));

      Search s = base.withMatchExpression(ex);
      Result<CloudSearchMetadata, List<Movie>> result = get(cloudSearch.search(s, movieParser));
      assertTrue("request failed", result.isSuccess());
      assertFalse("empty result", result.body().isEmpty());
    }

    @Test
    public void matchExpressionFieldAndNot() throws Exception {
      MatchExpression ex = field("title", "star wars").and(not(filterRange("year", 2000, 2012)));

      Search s = base.withMatchExpression(ex);
      Result<CloudSearchMetadata, List<Movie>> result = get(cloudSearch.search(s, movieParser));
      assertTrue("request failed", result.isSuccess());
      assertFalse("empty result", result.body().isEmpty());
    }

    @Test
    public void matchExpressionOR() throws Exception {
      MatchExpression ex = field("title", "star wars").or(field("title", "star trek"));

      Search s = base.withMatchExpression(ex);
      Result<CloudSearchMetadata, List<Movie>> result = get(cloudSearch.search(s, movieParser));
      assertTrue("request failed", result.isSuccess());
      assertFalse("empty result", result.body().isEmpty());
    }

    @Test
    public void matchExpressionComplexQuery() throws Exception {
      MatchExpression ex =
        (field("title", "Star Wars").or(field("title", "Star Trek")))
          .and(filterRange("year", 1980, 1990))
          .and(not(field("director", "Carpenter")))
          .and(not(field("title", "Spock")));

      List<Movie> expected = new ArrayList<Movie>() {{
        add(new Movie("tt0092007", Arrays.asList("Star Trek IV: The Voyage Home")));
        add(new Movie("tt0098382", Arrays.asList("Star Trek V: The Final Frontier")));
        add(new Movie("tt0084726", Arrays.asList("Star Trek: The Wrath of Khan")));
        add(new Movie("tt0086190", Arrays.asList("Star Wars: Episode VI - Return of the Jedi")));
        add(new Movie("tt0080684", Arrays.asList("Star Wars: Episode V - The Empire Strikes Back")));
      }};

      Search s = base.withMatchExpression(ex);
      Result<CloudSearchMetadata, List<Movie>> result = get(cloudSearch.search(s, movieParser));
      assertTrue("request failed", result.isSuccess());
      assertFalse("empty result", result.body().isEmpty());
      assertEquals("Unexpectes results number", expected.size(), result.body().size());
      assertTrue("Unexpected results",result.body().containsAll(expected));
    }

    @Test
    public void searchTwoResults() throws Exception {
      Search s = base
        .withQuery("star wars")
        .withSize(2);

      Result<CloudSearchMetadata, List<Movie>> result = get(cloudSearch.search(s, movieParser));
      assertTrue("request failed", result.isSuccess());
      assertFalse("empty result", result.body().isEmpty());
      assertEquals("Expected 2 results", 2, result.body().size());
    }

    @Test
    public void ignoreFirstResults() throws Exception {
      Search s = base
        .withQuery("star wars")
        .startAt(3);

      Result<CloudSearchMetadata, List<Movie>> result = get(cloudSearch.search(s, movieParser));
      assertTrue("request failed", result.isSuccess());
      assertFalse("empty result", result.body().isEmpty());
      assertEquals("Expected 4 results", 4, result.body().size());
    }

    @Test
    public void searchWithFacets() throws Exception {
      Search s = base
        .withQuery("star wars")
        .withFacets("genre");

      Result<CloudSearchMetadata, List<Movie>> result = get(cloudSearch.search(s, movieParser));
      assertTrue("request failed", result.isSuccess());
      assertFalse("empty result", result.body().isEmpty());
    }

    @Test
    public void searchWithFacetConstraints() throws Exception {
      Search s = base
        .withQuery("star wars")
        .withFacets("genre")
        .withFacetConstraints(new FacetConstraint("genre", "Action"));

      Result<CloudSearchMetadata, WithFacets<List<Movie>>> result = get(cloudSearch.searchWithFacets(s, movieParser));

      assertTrue("request failed", result.isSuccess());
      assertFalse("empty result", result.body().body().isEmpty());
    }


    @Test
    public void searchOpenIntervals() throws Exception {
      MatchExpression ex = field("title", "star wars")
        .and(not(filterTo("year", 2000)));

      Search s = base
        .withMatchExpression(ex);

      Result<CloudSearchMetadata, List<Movie>> result = get(cloudSearch.search(s, movieParser));

      assertTrue("request failed", result.isSuccess());
      assertFalse("empty result", result.body().isEmpty());
    }

    @Test
    public void sortFacetsByCountGenreDescending() throws Exception {
      Search s = base
        .withQuery("star wars")
        .withFacets("genre")
        .withFacetSorts(Sort.max("genre", Order.DESC()));

      Result<CloudSearchMetadata, List<Movie>> result = get(cloudSearch.search(s, movieParser));

      assertTrue("request failed", result.isSuccess());
      assertFalse("empty result", result.body().isEmpty());
    }

    @Test
    public void maxNumberOfFacetConstraints() throws Exception {
      Map<String, Integer> tops = new HashMap<String, Integer>(){{
        put("genre", 2);
      }};

      Search s = base
        .withQuery("star wars")
        .withFacets("genre")
        .withFacetTops(tops);

      Result<CloudSearchMetadata, WithFacets<List<Movie>>> result = get(cloudSearch.searchWithFacets(s, movieParser));

      assertTrue("request failed", result.isSuccess());
      assertFalse("empty result", result.body().body().isEmpty());

      List<String> constraints = new ArrayList<String>();

      for(Facet f : result.body().facets()){
        constraints.addAll(f.constraints().keySet());
      }

      assertEquals("Unexpected constraints result", 2, constraints.size());

    }

}
