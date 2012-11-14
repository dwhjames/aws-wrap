package com.pellucid.aws.simpledb;

import java.util.ArrayList;
import java.util.List;

import scala.collection.JavaConversions;
import scala.collection.Seq;

import play.libs.Scala;

public class SDBExpected {

    private final String name;
    private final String value;

    public SDBExpected(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public String name() {
        return this.name;
    }

    public String value() {
        return this.value;
    }

    aws.simpledb.SDBExpected asScala() {
        return new aws.simpledb.SDBExpected(name, Scala.Option(value));
    }

    static Seq<aws.simpledb.SDBExpected> listAsScalaSeq(List<SDBExpected> expected) {
        List<aws.simpledb.SDBExpected> scalaJList = new ArrayList<aws.simpledb.SDBExpected>();
        for (SDBExpected exp: expected) {
            scalaJList.add(exp.asScala());
        }
        return JavaConversions.asScalaBuffer(scalaJList).toList();
    }

}
