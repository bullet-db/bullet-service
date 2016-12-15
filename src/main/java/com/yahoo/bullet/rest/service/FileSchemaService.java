/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.google.gson.reflect.TypeToken;
import com.yahoo.bullet.rest.schema.Column;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;

@Slf4j
public class FileSchemaService extends SchemaService {
    /**
     * Loads the contents of a file. The file is expected to contain a JSON array with entries in the following format:
     * <pre>
     * {@code
     *     {
     *         "name": "the column name",
     *         "type": "one of ("BOOLEAN", "LONG", "DOUBLE", "STRING", "MAP", "LIST")",
     *         "subtype": "required for MAP/LIST type. One of ("BOOLEAN", "LONG", "DOUBLE", "STRING") if type is MAP
     *                     or MAP if type is LIST",
     *         "description": "an optional description of this column",
     *         "enumerations": [ include known subfields in this column (must be of type MAP) of the form
     *                           {"name": "the name of the subfield", "description": "a description for it"}
     *                         ]
     *     }
     * }
     * </pre>
     *
     * @param version The schema version.
     * @param path The path to the file.
     */
    public FileSchemaService(String version, String path) {
        super(version);
        setSchema(getColumns(path));
    }

    private List<Column> getColumns(String path) {
        log.info("Parsing json from read file");
        List<Column> columns = GSON.fromJson(getReader(path), new TypeToken<List<Column>>() { }.getType());
        log.info("Read columns : " + columns);
        return columns;
    }

    private Reader getReader(String path) {
        log.info("Loading columns from file : " + path);
        try {
            return new FileReader(new File(path));
        } catch (IOException ioe) {
            log.warn("Unable to read from file path. Trying classpath instead...", ioe);
            return new InputStreamReader(getClass().getResourceAsStream(path));
        }
    }
}
