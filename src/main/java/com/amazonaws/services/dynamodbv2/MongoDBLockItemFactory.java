package com.amazonaws.services.dynamodbv2;

import org.bson.Document;

public interface MongoDBLockItemFactory {
    LockItem create(Document item);
}

