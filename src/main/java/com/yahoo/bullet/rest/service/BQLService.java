/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.bql.BQLResult;
import com.yahoo.bullet.bql.BulletQueryBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Service
public class BQLService {
    private BulletQueryBuilder bulletQueryBuilder;

    /**
     * Constructor that takes a BQL query builder.
     *
     * @param bulletQueryBuilder The {@link BulletQueryBuilder} to use.
     */
    @Autowired
    public BQLService(BulletQueryBuilder bulletQueryBuilder) {
        Objects.requireNonNull(bulletQueryBuilder);
        this.bulletQueryBuilder = bulletQueryBuilder;
    }

    /**
     * Convert this BQL query to a valid Bullet Query or error out.
     *
     * @param bql The query to convert.
     * @return The {@link BQLResult} containing a query or errors.
     */
    public BQLResult toQuery(String bql) {
        return bulletQueryBuilder.buildQuery(bql);
    }
}
