/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.util.Objects;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * A {@code rest} read straight from the {@code _timeseries} source under one exclusion set.
 * Exclusions are pushed into the loader, so the column never needs computed edits and works
 * on every transport version. Several rests under different exclusions coexist; each has
 * its own derived plan name and may overlap the promoted labels.
 */
public record SourcePacking(Attribute attribute, Set<String> excluded) implements LabelColumn {

    public SourcePacking {
        Objects.requireNonNull(attribute, "attribute");
        excluded = Set.copyOf(Objects.requireNonNull(excluded, "excluded"));
    }

    @Override
    public boolean sourceBacked() {
        return true;
    }

    @Override
    public LabelColumn remap(UnaryOperator<Attribute> mapping) {
        return new SourcePacking(mapping.apply(attribute), excluded);
    }
}
