/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.yahoo.bullet.rest.schema.JSONAPIDocument;
import com.yahoo.bullet.rest.schema.JSONAPIField;
import com.yahoo.bullet.typesystem.Schema;
import com.yahoo.bullet.typesystem.Schema.Field;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

/**
 * Manages access to and provides the schema for the fields. Use {@link #setSchema(List)} to set the schema.
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
 *              "name": "the field name",
 *              "type": "one of BOOLEAN, INTEGER, LONG, FLOAT, DOUBLE, STRING, BOOLEAN_MAP, INTEGER_MAP, LONG_MAP,
 *                       FLOAT_MAP, DOUBLE_MAP, STRING_MAP, BOOLEAN_MAP_MAP, INTEGER_MAP_MAP, LONG_MAP_MAP,
 *                       FLOAT_MAP_MAP, DOUBLE_MAP_MAP, STRING_MAP_MAP, BOOLEAN_LIST, INTEGER_LIST, LONG_LIST,
 *                       FLOAT_LIST, DOUBLE_LIST, STRING_LIST, BOOLEAN_MAP_LIST, INTEGER_MAP_LIST, LONG_MAP_LIST,
 *                       FLOAT_MAP_LIST, DOUBLE_MAP_LIST, STRING_MAP_LIST"
 *              "description": "an optional description of this column",
 *              "subFields": [ include enumerated subfields (field must be of type MAP) of the form
 *                  {"name": "the name of the subfield", "description": "a description for it"}
 *              ]
 *              "subSubFields": [ include enumerated subSubfields (field must be of type MAP_MAP) of the form
 *                  {"name": "the name of the subsubfield", "description": "a description for it"}
 *              ]
 *              "subListFields": [ include enumerated subListfields (field must be of type MAP_LIST) of the form
 *                  {"name": "the name of the sublistfield", "description": "a description for it"}
 *              ]
 *         }
 *     }
 * }
 * </pre>
 */
@Slf4j
@Service
@Getter
public class SchemaService {
    private String schema;
    private String version;

    public static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();
    public static final String VERSION_KEY = "version";

    /**
     * Constructor that takes in a a string file path and a version string and loads the contents of the file.
     *
     * The file is expected to contain a JSON array with entries in the following format:
     *
     * <pre>
     * {@code
     *     {
     *         "name": "the column name",
     *         "type": "one of ("BOOLEAN", "INTEGER", "LONG", "FLOAT", "DOUBLE", "STRING", "MAP", "LIST")",
     *         "subtype": "required for MAP/LIST type. One of ("BOOLEAN", "INTEGER", "LONG", "FLOAT", "DOUBLE",
     *                    "STRING", "MAP") if type is LIST or MAP",
     *         "description": "an optional description of this column",
     *         "enumerations": [ include known subfields in this column (must be of type MAP) of the form
     *                           {"name": "the name of the subfield", "description": "a description for it"}
     *                         ]
     *     }
     * }
     * </pre>
     *
     * @param version The schema version.
     * @param filePath The filePath to the file.
     */
    @Autowired
    public SchemaService(@Value("${bullet.schema.version}") String version, @Value("${bullet.schema.file}") String filePath) {
        this.version = version;
        setSchema(loadFields(filePath));
    }

    /**
     * Sets the schema from a List of {@link Field}. It validates the fields and throws a RuntimeException if any.
     * It validates and converts the Field to match the JSON API specification.
     *
     * @param fields The List of fields that constitute the schema.
     */
    public final void setSchema(List<Field> fields) {
        Objects.requireNonNull(fields);
        List<JSONAPIField> mapped = fields.stream().map(JSONAPIField::from).collect(toList());
        JSONAPIDocument schema = new JSONAPIDocument(mapped, Collections.singletonMap(VERSION_KEY, getVersion()));
        this.schema = GSON.toJson(schema);
        log.info("Schema: {}", this.schema);
    }

    private List<Field> loadFields(String path) {
        Schema schema = new Schema(path);
        log.info("Read {} fields", schema.size());
        return schema.getFields();
    }
}
