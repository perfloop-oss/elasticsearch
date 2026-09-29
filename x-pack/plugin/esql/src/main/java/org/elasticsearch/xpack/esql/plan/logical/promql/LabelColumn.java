/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * One {@code rest} column of a translated table: the packed/dynamic dictionary holding every remaining label.
 * <p>
 * Like ClickHouse, a table carries promoted labels directly plus rests; a {@code rest} may overlap the promoted
 * names. Upstream cannot edit a packed column, so older clusters carry several rests at once (one per exclusion
 * set). The two implementations cover how the record is read: {@link SourcePacking} reads {@code _timeseries}
 * variants straight from the source and works on every transport version, while {@link PackedRecord} is the packed
 * JSON record that newer data nodes understand and edit in place.
 */
public sealed interface LabelColumn permits SourcePacking, PackedRecord {

    /** The plan column carrying this record variant. */
    Attribute attribute();

    /** The exclusion set this column embodies; empty means the complete record. */
    Set<String> excluded();

    /**
     * Whether exclusions can be pushed into the loader (source-backed columns) or must be
     * applied as computed edits downstream.
     */
    boolean sourceBacked();

    /** Rebuilds this column over a remapped attribute (e.g. self-join reidentification assigns fresh ids). */
    LabelColumn remap(UnaryOperator<Attribute> mapping);
}
