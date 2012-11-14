package com.pellucid.aws.simpledb;

import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.List;
import java.util.ArrayList;


public class SDBAttribute {

    private final String name;
    private final String value;
    private final boolean replace;
    
    public SDBAttribute(String name, String value) {
        this(name, value, false);
    }
    
    public SDBAttribute(String name, String value, boolean replace) {
        this.name = name;
        this.value = value;
        this.replace = replace;
    }

    public String name() {
        return this.name;
    }

    public String value() {
        return this.value;
    }
    
    public boolean replace() {
        return this.replace;
    }

    aws.simpledb.SDBAttribute asScala() {
        return new aws.simpledb.SDBAttribute(name, value, replace);
    }

    static Seq<aws.simpledb.SDBAttribute> listAsScalaSeq(List<SDBAttribute> attributes) {
        List<aws.simpledb.SDBAttribute> scalaJList = new ArrayList<aws.simpledb.SDBAttribute>();
        for (SDBAttribute attr: attributes) {
            scalaJList.add(attr.asScala());
        }
        return JavaConversions.asScalaBuffer(scalaJList).toList();
    }

}
