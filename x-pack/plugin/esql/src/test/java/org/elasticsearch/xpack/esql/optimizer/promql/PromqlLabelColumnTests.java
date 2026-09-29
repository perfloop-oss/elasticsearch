/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.JsonRemovePath;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.JsonSet;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

/**
 * Singleton enforcement for dual label materialization (promoted labels plus {@code rest} columns, overlapping
 * allowed): beginning with {@code esql_timeseries_metadata_attribute_v2} a translation carries at most one
 * {@code rest} (the packed JSON record, edited in place) and emits no source packings; older versions stay on the
 * old model (one rest per exclusion set, no JSON machinery). Versions are pinned explicitly so both models stay
 * covered deterministically.
 */
public class PromqlLabelColumnTests extends AbstractPromqlPlanOptimizerTests {

    private static final List<String> QUERIES = List.of(
        "network.cost",
        "sum without (pod) (network.cost)",
        "sum by (pod) (network.cost)",
        "sum by (pod) (sum without (pod) (network.cost))",
        // Dual grouping: the inner without groups by both the promoted pod and the rest excluding region.
        "sum by (pod) (sum without (region) (network.cost))",
        "sum by (dst) (label_replace(network.cost, \"dst\", \"$1\", \"pod\", \"(.+)\"))",
        "sum by (dst) (label_join(network.cost, \"dst\", \"-\", \"pod\", \"region\"))",
        "network.bytes_in * 8"
    );

    public PromqlLabelColumnTests(VersionMode versionMode) {
        super(versionMode);
    }

    private static TransportVersion previous() {
        return TransportVersionUtils.getPreviousVersion(FieldAttribute.ESQL_TIMESERIES_METADATA_ATTRIBUTE_V2);
    }

    private LogicalPlan analyze(String expression, TransportVersion version) {
        String query = "PROMQL index=k8s start=\"2024-05-10T00:00:00Z\" end=\"2024-05-10T01:00:00Z\" step=1m (" + expression + ")";
        return analyzerWithEnrichPolicies().addK8s()
            .unmappedResolution(UnmappedResolution.NULLIFY)
            .minimumTransportVersion(version)
            .query(query);
    }

    public void testOldVersionsUseNoJsonMachinery() {
        for (String expression : QUERIES) {
            LogicalPlan plan = analyze(expression, previous());
            var json = new ArrayList<Expression>();
            plan.forEachExpressionDown(
                Expression.class,
                value -> { if (value instanceof JsonSet || value instanceof JsonRemovePath) json.add(value); }
            );
            assertThat(expression, json, empty());
        }
    }

    public void testOldVersionsPackExclusionsAtTheSource() {
        LogicalPlan plan = analyze("sum without (pod) (network.cost)", previous());
        var packings = plan.collect(EsRelation.class)
            .stream()
            .flatMap(relation -> relation.output().stream())
            .filter(TimeSeriesMetadataAttribute.class::isInstance)
            .map(TimeSeriesMetadataAttribute.class::cast)
            .filter(packing -> packing.excludedFields().contains("pod"))
            .toList();
        assertThat(packings, not(empty()));
    }

    public void testNewVersionsEmitNoPackings() {
        for (String expression : QUERIES) {
            LogicalPlan plan = analyze(expression, TransportVersion.current());
            assertThat(expression, collectPackings(plan), empty());
        }
    }

    private static List<TimeSeriesWithout> collectPackings(LogicalPlan plan) {
        var packings = new ArrayList<TimeSeriesWithout>();
        plan.forEachExpressionDown(TimeSeriesWithout.class, packings::add);
        return packings;
    }

    public void testNewVersionsCarryAtMostOneRecordPerRelation() {
        for (String expression : QUERIES) {
            LogicalPlan plan = analyze(expression, TransportVersion.current());
            for (EsRelation relation : plan.collect(EsRelation.class)) {
                long records = relation.output().stream().filter(TimeSeriesMetadataAttribute.class::isInstance).count();
                assertThat(expression, records, lessThanOrEqualTo(1L));
            }
        }
    }
}
