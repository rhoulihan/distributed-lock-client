package com.amazonaws.services.dynamodbv2;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.bson.Document;

import com.mongodb.client.MongoCursor;


public class MongoDBLockItemIterator extends LockItemIterator{
    private MongoCursor<Document> cursor;
    private MongoDBLockItemFactory lockItemFactory;
    protected List<LockItem> currentPageResults = Collections.emptyList();
    protected int currentPageResultsIndex = 0;

    MongoDBLockItemIterator(final MongoCursor<Document> cursor, final MongoDBLockItemFactory lockItemFactory) {
        this.cursor = Objects.requireNonNull(cursor, "cursor must not be null");
        this.lockItemFactory = Objects.requireNonNull(lockItemFactory, "lockItemFactory must not be null");
    }

    @Override
    protected boolean hasAnotherPageToLoad() {
        return cursor.hasNext();
    }

    @Override
    protected boolean hasLoadedFirstPage() {
        return !this.currentPageResults.isEmpty();
    }

    @Override
    protected void loadNextPageIntoResults() {
        this.currentPageResults.clear();
        while (this.currentPageResults.size() < 10) {
            if (!cursor.hasNext()) {
                break;
            }
            
            this.currentPageResults.add(this.lockItemFactory.create(cursor.next()));
        }
    }
}
