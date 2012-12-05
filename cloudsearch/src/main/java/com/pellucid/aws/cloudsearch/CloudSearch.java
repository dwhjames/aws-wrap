/*
package com.pellucid.aws.cloudsearch;

import scala.concurrent.Future;

import com.pellucid.aws.cloudsearch.models.*;

import aws.core.Result;
import aws.core.parsers.Parser;

import aws.cloudsearch.CloudSearchMetadata;

public class CloudSearch {

  private aws.cloudsearch.CloudSearch delegate;
  private final aws.cloudsearch.CloudSearchRegion scalaRegion;

  public CloudSearch(){
    this.scalaRegion = aws.cloudsearch.CloudSearchRegion$.MODULE$.DEFAULT();
  }

  public CloudSearch(aws.cloudsearch.CloudSearchRegion scalaRegion){
    this.scalaRegion = scalaRegion;
  }

  public <T> Future<Result<aws.cloudsearch.CloudSearchMetadata,T>> search(Search search, Parser<Result<CloudSearchMetadata, T>> p){
    return delegate.search(aws.cloudsearch.Search.fromJava(search), scalaRegion, p);
  }
}
*/