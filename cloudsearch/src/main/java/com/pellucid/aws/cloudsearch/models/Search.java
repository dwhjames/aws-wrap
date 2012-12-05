package com.pellucid.aws.cloudsearch.models;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;

public class Search {

  private Domain domain;
  private String query;
  private MatchExpression matchExpression;
  private List<String> returnFields;
  private List<String> facets;
  private List<FacetConstraint> facetConstraints;
  private List<Sort> facetSort;
  private Map<String, Integer> facetTops;
  private List<Rank> ranks;
  private Map<String, Range> scores;
  private int size;
  private int startAt;

  public Search(Domain domain) {
    this.domain = domain;
  }

  public Search(
    Domain domain,
    String query,
    MatchExpression matchExpression,
    List<String> returnFields,
    List<String> facets,
    List<FacetConstraint> facetConstraints,
    List<Sort> facetSort,
    Map<String, Integer> facetTops,
    List<Rank> ranks,
    Map<String, Range> scores,
    int size,
    int startAt
  ){
    this.domain = domain;
    this.query = query;
    this.matchExpression = matchExpression;
    this.returnFields = returnFields;
    this.facets = facets;
    this.facetConstraints = facetConstraints;
    this.facetSort = facetSort;
    this.facetTops = facetTops;
    this.ranks = ranks;
    this.scores = scores;
    this.size = size;
    this.startAt = startAt;
  }


  public Domain getDomain(){ return domain;}
  public String getQuery(){ return query;}
  public MatchExpression getMatchExpression(){ return matchExpression;}
  public List<String> getReturnFields(){ return returnFields;}
  public List<String> getFacets(){ return facets;}
  public List<FacetConstraint> getFacetConstraints(){ return facetConstraints;}
  public List<Sort> getFacetSort(){ return facetSort;}
  public Map<String, Integer> getFacetTops(){ return facetTops;}
  public List<Rank> getRanks(){ return ranks;}
  public Map<String, Range> getScores(){ return scores;}
  public int getSize(){ return size;}
  public int getStartAt(){ return startAt;}


  public Search withQuery(String query) {
    return new Search(domain, query, matchExpression, returnFields, facets, facetConstraints, facetSort, facetTops, ranks, scores, size, startAt);
  }

  public Search withMatchExpression(MatchExpression matchExpression) {
    return new Search(domain, query, matchExpression, returnFields, facets, facetConstraints, facetSort, facetTops, ranks, scores, size, startAt);
  }

  public Search withReturnFields(String... returnFields) {
    return new Search(domain, query, matchExpression, Arrays.asList(returnFields), facets, facetConstraints, facetSort, facetTops, ranks, scores, size, startAt);
  }

  public Search withFacetConstraints(FacetConstraint... facetConstraints) {
    return new Search(domain, query, matchExpression, returnFields, facets, Arrays.asList(facetConstraints), facetSort, facetTops, ranks, scores, size, startAt);
  }

  public Search withFacetSorts(Sort... facetSort) {
    return new Search(domain, query, matchExpression, returnFields, facets, facetConstraints, Arrays.asList(facetSort), facetTops, ranks, scores, size, startAt);
  }

  public Search withFacetTops(Map<String, Integer> facetTops){
    return new Search(domain, query, matchExpression, returnFields, facets, facetConstraints, facetSort, facetTops, ranks, scores, size, startAt);
  }

  public Search withRanks(Rank... ranks) {
    return new Search(domain, query, matchExpression, returnFields, facets, facetConstraints, facetSort, facetTops, Arrays.asList(ranks), scores, size, startAt);
  }

  public Search withScores(Map<String, Range> scores) {
    return new Search(domain, query, matchExpression, returnFields, facets, facetConstraints, facetSort, facetTops, ranks, scores, size, startAt);
  }

  public Search withSize(int size) {
    return new Search(domain, query, matchExpression, returnFields, facets, facetConstraints, facetSort, facetTops, ranks, scores, size, startAt);
  }

  public Search startAt(int startAt) {
    return new Search(domain, query, matchExpression, returnFields, facets, facetConstraints, facetSort, facetTops, ranks, scores, size, startAt);
  }

  public Search withFacets(String... facets){
    return new Search(domain, query, matchExpression, returnFields, Arrays.asList(facets), facetConstraints, facetSort, facetTops, ranks, scores, size, startAt);
  }

}
