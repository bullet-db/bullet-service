/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.schema;

import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.yahoo.bullet.typesystem.Type.BOOLEAN;
import static com.yahoo.bullet.typesystem.Type.DOUBLE;
import static com.yahoo.bullet.typesystem.Type.FLOAT;
import static com.yahoo.bullet.typesystem.Type.INTEGER;
import static com.yahoo.bullet.typesystem.Type.LIST;
import static com.yahoo.bullet.typesystem.Type.LONG;
import static com.yahoo.bullet.typesystem.Type.MAP;
import static com.yahoo.bullet.typesystem.Type.STRING;
import static java.util.Arrays.asList;

@NoArgsConstructor
@Getter @Setter
@Slf4j
public class Column {
    public static final Set<Type> TYPES = new HashSet<>(asList(BOOLEAN, INTEGER, LONG, FLOAT, DOUBLE, STRING, MAP, LIST));
    public static final Set<Type> PRIMITIVES = new HashSet<>(Type.PRIMITIVES);

    private String name;
    private Type type;
    private Type subtype;
    private String description;
    private List<Enumeration> enumerations;

    /**
     * Checks and returns true if this column is valid.
     *
     * @return boolean denoting whether this column definition is correct.
     */
    public boolean isValid() {
        // TODO: Consider replacing with JSON Schema based validation and provide a compile time way to hook into
        // the validation. We lose the tying to Bullet Type however.
        if (name == null || name.isEmpty()) {
            log.error("Name must be provided");
            return false;
        }
        if (!TYPES.contains(type)) {
            log.error("Unknown type: " + type);
            return false;
        }
        if (PRIMITIVES.contains(type) && subtype != null) {
            log.error("Subtype can only be set if type is not a complex type");
            return false;
        }
        if (type == MAP && (subtype == null || (!PRIMITIVES.contains(subtype) && subtype != MAP))) {
            log.error("Subtype must be set to a primitive or MAP if type is MAP");
            return false;
        }
        if (type == LIST && (subtype == null || (!PRIMITIVES.contains(subtype) && subtype != MAP))) {
            log.error("Subtype must be set to a primitive or MAP if type is LIST");
            return false;
        }
        if (type != MAP && enumerations != null) {
            log.error("Enumerations are only supported for MAP type columns");
            return false;
        }
        return true;
    }

}
