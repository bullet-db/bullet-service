/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.model;

import com.yahoo.bullet.typesystem.Schema.Field;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Getter @Setter
public class JSONAPIField {
    private String id;
    private String type = "column";
    private Field attributes;

    /**
     * Creates an instance from a {@link Field}. Does not care if the field is valid.
     *
     * @param field The field to convert.
     * @return The created instance.
     */
    public static JSONAPIField from(Field field) {
        JSONAPIField jsonAPIField = new JSONAPIField();
        jsonAPIField.setAttributes(field);
        jsonAPIField.setId(field.getName());
        return jsonAPIField;
    }
}
