/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.schema;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Getter @Setter
public class JSONAPIColumn {
    private String id;
    private String type = "column";
    private Column attributes;

    /**
     * Creates a JSONAPIColumn from a {@link Column}. Does not care if the Column is valid.
     *
     * @param column The Column to convert.
     * @return The created JSONAPIColumn.
     */
    public static JSONAPIColumn from(Column column) {
        JSONAPIColumn jsonAPIColumn = new JSONAPIColumn();
        jsonAPIColumn.setAttributes(column);
        jsonAPIColumn.setId(column.getName());
        return jsonAPIColumn;
    }
}
