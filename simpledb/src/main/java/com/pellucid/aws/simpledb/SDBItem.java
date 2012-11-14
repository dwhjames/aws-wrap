package com.pellucid.aws.simpledb;

import java.util.List;
import java.util.ArrayList;

import scala.collection.JavaConversions;
import scala.collection.Seq;

public class SDBItem {

    private final String name;
    private final List<SDBAttribute> attributes;

    public SDBItem(String name) {
        this.name = name;
        this.attributes = new ArrayList<SDBAttribute>();
    }

    public SDBItem(String name, List<SDBAttribute> attributes) {
        this.name = name;
        this.attributes = new ArrayList<SDBAttribute>(attributes);
    }

    public String name() {
        return name;
    }

    public List<SDBAttribute> attributes() {
        return new ArrayList<SDBAttribute>(this.attributes);
    }

    static List<SDBItem> listFromScalaSeq(Seq<aws.simpledb.SDBItem> scalaItems) {
        List<SDBItem> result = new ArrayList<SDBItem>();
        for (aws.simpledb.SDBItem item: JavaConversions.asJavaList(scalaItems)) {
            result.add(new SDBItem(item.name(), SDBAttribute.listFromScalaSeq(item.attributes())));
        }
        return result;
    }

}
