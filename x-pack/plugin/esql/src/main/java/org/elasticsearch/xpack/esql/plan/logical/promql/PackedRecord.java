/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;

import java.util.Objects;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * The packed JSON {@code rest}: one column holding the complete current record, edited in
 * place as labels are projected or replaced. Only data nodes on
 * {@code esql_timeseries_metadata_attribute_v2} and newer understand the record edit nodes; older
 * clusters stay on {@link SourcePacking} rests instead, one per exclusion set.
 */
public record PackedRecord(Attribute attribute, Set<String> excluded) implements LabelColumn {

    public PackedRecord {
        Objects.requireNonNull(attribute, "attribute");
        excluded = Set.copyOf(Objects.requireNonNull(excluded, "excluded"));
    }

    @Override
    public boolean sourceBacked() {
        return attribute instanceof TimeSeriesMetadataAttribute;
    }

    @Override
    public LabelColumn remap(UnaryOperator<Attribute> mapping) {
        return new PackedRecord(mapping.apply(attribute), excluded);
    }
}
