/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest;

import com.yahoo.bullet.storage.NullStorageManager;
import com.yahoo.bullet.storage.StorageManager;
import org.testng.Assert;
import org.testng.annotations.Test;

public class StorageConfigurationTest {
    @Test
    public void testStorageManager() {
        StorageConfiguration configuration = new StorageConfiguration();
        StorageManager manager = configuration.storageManager("test_storage_defaults.yaml");
        Assert.assertTrue(manager instanceof NullStorageManager);
    }
}
