/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.model;

import com.yahoo.bullet.rest.model.JSONAPIField;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;
import java.util.Map;

@NoArgsConstructor @AllArgsConstructor
@Getter @Setter
public class JSONAPIDocument {
    private List<JSONAPIField> data;
    private Map<String, Object> meta;
}
