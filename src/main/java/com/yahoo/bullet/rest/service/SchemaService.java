/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.yahoo.bullet.rest.schema.Column;
import com.yahoo.bullet.rest.schema.JSONAPIDocument;
import com.yahoo.bullet.rest.schema.JSONAPIColumn;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

/**
 * Manages access to and provides the schema for the columns. Use {@link #setSchema(List)} to set the schema.
 *
 * The schema is of the form:
 * <pre>
 * {@code
 *     {
 *         "meta": { meta information },
 *         "data": [ the columns ]
 *     }
 * Each entry in data is of the form:
 *     {
 *         "id": "a string id",
 *         "type": "column",
 *         "attributes": {
 *              "name": "the column name",
 *              "type": "one of ("BOOLEAN", "LONG", "DOUBLE", "STRING", "MAP", "LIST")",
 *              "subtype": "required for MAP/LIST type. One of ("BOOLEAN", "LONG", "DOUBLE", "STRING") if type is MAP
 *                          or MAP if type is LIST"
 *              "description": "an optional description of this column",
 *              "enumerations": [ include enumerated subfields (column must be of type MAP) of the form
 *                  {"name": "the name of the subfield", "description": "a description for it"}
 *              ]
 *         }
 *     }
 * }
 * </pre>
 */
@Service
@Slf4j
public abstract class SchemaService {
    public static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();
    @Getter
    private String schema;
    @Getter
    private String version;

    public static final String VERSION_KEY = "version";

    /**
     * Constructor that takes in a version string.
     *
     * @param version The schema version.
     */
    public SchemaService(String version) {
        this.version = version;
    }

    /**
     * Sets the schema from a List of {@link Column}. It validates the columns and throws a RuntimeException if any.
     * It validates and converts the Column to match the JSON API specification.
     *
     * @param columns The List of columns that constitute the schema.
     */
    public final void setSchema(List<Column> columns) {
        Objects.requireNonNull(columns);
        List<JSONAPIColumn> mapped = columns.stream().map(SchemaService::convert).collect(toList());
        JSONAPIDocument schema = new JSONAPIDocument(mapped, Collections.singletonMap(VERSION_KEY, getVersion()));
        this.schema = GSON.toJson(schema);
        log.info("Schema: " + this.schema);
    }

    private static JSONAPIColumn convert(Column column) {
        Objects.requireNonNull(column);
        if (!column.isValid())  {
            throw new RuntimeException("Invalid column: " + GSON.toJson(column));
        }
        return JSONAPIColumn.from(column);
    }
}
