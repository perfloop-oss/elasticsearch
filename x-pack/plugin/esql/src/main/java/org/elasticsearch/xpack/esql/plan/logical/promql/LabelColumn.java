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
 * Like ClickHouse, a table carries promoted labels directly plus one {@code rest} per exclusion set; a {@code rest}
 * may overlap the promoted names. Upstream cannot edit a packed column, so the pipeline carries several rests at
 * once (one per exclusion set); a later restriction narrows the set to a single metadata column. {@link SourcePacking}
 * reads {@code _timeseries} variants straight from the source and works on every transport version; the packed JSON
 * record variant lives in the next PR.
 */
public sealed interface LabelColumn permits SourcePacking {

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
