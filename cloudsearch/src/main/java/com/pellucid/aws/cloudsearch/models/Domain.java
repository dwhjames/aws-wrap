package com.pellucid.aws.cloudsearch.models;

public class Domain {
  private String name;
  private String id;

  public Domain(String name, String id) {
    this.name = name;
    this.id = id;
  }

  public String getId() {
    return this.id;
  }

  public String getName() {
    return this.name;
  }
}