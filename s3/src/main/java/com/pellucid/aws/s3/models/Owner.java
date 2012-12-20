package com.pellucid.aws.s3.models;

import play.libs.Scala;

public class Owner {

    private aws.s3.models.Owner scalaOwner;

    private Owner(aws.s3.models.Owner scalaOwner) {
        this.scalaOwner = scalaOwner;
    }

    public String id() {
        return this.scalaOwner.id();
    }

    public String name() {
        return Scala.orNull(this.scalaOwner.name());
    }

    public static Owner fromScala(aws.s3.models.Owner scalaOwner) {
        return new Owner(scalaOwner);
    }

}
