package com.pellucid.aws.s3.models;

import java.util.List;

public class BatchDeletion {

    public static class DeletionSuccess {
        
    }
    
    public static class DeletionFailure {
        
    }
    
    private aws.s3.models.BatchDeletion scalaDeletion;

    public BatchDeletion(aws.s3.models.BatchDeletion scalaDeletion) {
        this.scalaDeletion = scalaDeletion;
    }

    public List<DeletionSuccess> successes() {
        return null;
    }

    public List<DeletionFailure> failures() {
        return null;
    }

    public static BatchDeletion fromScala(aws.s3.models.BatchDeletion scalaDeletion) {
        return new BatchDeletion(scalaDeletion);
    }

}
