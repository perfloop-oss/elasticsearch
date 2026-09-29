/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.util.Objects;
import java.util.function.UnaryOperator;

/**
 * One promoted label of a translated table: a label carried directly under its canonical name.
 * <p>
 * Like ClickHouse, a table carries promoted labels directly plus {@code rest} columns holding every remaining
 * label; a {@code rest} may overlap the promoted names. Promoted columns are tracked in header order.
 *
 * @param name the canonical label name
 * @param attribute the plan column carrying it
 */
public record PromotedColumn(String name, Attribute attribute) {

    public PromotedColumn {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(attribute, "attribute");
    }

    /** Rebuilds this column over a remapped attribute (e.g. self-join reidentification assigns fresh ids). */
    public PromotedColumn remap(UnaryOperator<Attribute> mapping) {
        return new PromotedColumn(name, mapping.apply(attribute));
    }
}
