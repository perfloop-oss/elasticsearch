/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.grouping.TStep;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.JsonRemovePath;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.JsonSet;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.parser.promql.PromqlLogicalPlanBuilder;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnpackDims;
import org.elasticsearch.xpack.esql.plan.logical.join.InnerJoin;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult.Kind;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryComparison;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryOperator;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinarySet;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LiteralSelector;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAndNullable;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels.PROMETHEUS_LABELS_PREFIX;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.intersect;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.open;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.project;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.emitNullExpression;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.find;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.finestFirst;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.mapFinite;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.mapOpen;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.mapToRef;
import static org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch.Joining;

/**
 * Shared state and assembly helpers for one PromQL translation. Nodes own their lowering, while this context
 * preserves the existing header bookkeeping, aggregation boundaries, and command finalization.
 */
public record TranslationContext(
    PromqlCommand cmd,
    AnalyzerContext analyzer,
    /* Alias for the step bucket expression used in all aggregation groupings. May be null for empty indices. */
    Alias stepBucketAlias,
    /* The columns the result subtree MUST expose. */
    TranslationConstraint required,
    /* The current evaluation time (default: @timestamp). */
    Expression time
) {
    // Sentinel bounds for open-ended range queries (PROMQL step=X without explicit start/end): TStep requires explicit bounds,
    // so pass the widest representable range. EPOCH/MAX_MILLIS_BEFORE_9999 avoid time boundary handling in the engine.
    private static final Instant EPOCH_MIN = Instant.EPOCH;
    private static final Instant EPOCH_MAX = Instant.ofEpochMilli(DateUtils.MAX_MILLIS_BEFORE_9999);

    /** The command exposes every label of the result series: the full open label space. */
    public TranslationContext(PromqlCommand cmd, AnalyzerContext analyzer) {
        this(cmd, analyzer, null, open(), null);
    }

    /** Translate an independent child without changing the enclosing branch's timing or aggregation state. */
    public TranslationContext withRequired(TranslationConstraint childRequired) {
        return new TranslationContext(cmd, analyzer, stepBucketAlias, childRequired, time);
    }

    /**
     * Dispatches to the node that owns the translation. The source relation is the leaf of the produced ES|QL subtree;
     * requirements travel down the PromQL tree and intermediate results travel back up.
     */
    public IntermediateResult translate(LogicalPlan node) {
        if (node instanceof PromqlPlan promql) {
            return promql.translate(this);
        }
        throw new QlIllegalArgumentException("Unsupported PromQL plan node: {}", node);
    }

    public Configuration configuration() {
        return analyzer.configuration();
    }

    /**
     * Whether the packed JSON label record may be used here: every node in the cluster must understand the record
     * edit nodes. Older clusters stay on one {@link SourcePacking} column per exclusion set instead.
     */
    public boolean supportsPackedRecords() {
        return analyzer.minimumVersion().supports(FieldAttribute.ESQL_TIMESERIES_METADATA_ATTRIBUTE_V2);
    }

    public Attribute stepAttr() {
        return stepBucketAlias != null ? stepBucketAlias.toAttribute() : cmd.stepAttribute();
    }

    /** Translates one merge branch with its own step bucket and evaluation time. */
    public IntermediateResult translateIntermediate(LogicalPlan branch, NameId stepId, NameId valueId) {
        Expression branchTime = cmd.collectEvaluationTimestampForBranch(branch);
        Alias step = canCreateStepBucket() ? emitStepBucketExpression(stepId, branchTime) : null;
        var run = new TranslationContext(cmd, analyzer, step, required, branchTime);
        return run.translateIntermediate(branch, valueId);
    }

    /** Finishes the command, including independently translated union branches and the declared output projection. */
    public LogicalPlan translateFinal() {
        if (cmd.promqlPlan() instanceof VectorBinaryOperator op) {
            VectorMatch match = op.match();
            if (match.filter() != VectorMatch.Filter.NONE || match.grouping() != Joining.NONE) {
                return doTranslateFinal(op.translateJoin(this).plan(), false);
            }
        }

        // `or` is the only set operator that adds rows (more series), requiring a top-level multi-branch `UnionAll` that
        // cannot compose as a single-value sub-expression.
        // PromQL `or` is left-associative, so flatten the top-level chain into independent branches.
        var branches = new ArrayList<LogicalPlan>();
        flattenUnion(cmd.promqlPlan(), branches);

        if (branches.size() == 1) {
            IntermediateResult intermediateResult = translateIntermediate(cmd.promqlPlan(), cmd.stepId(), cmd.valueId());
            Attribute declared = find(cmd.output(), mapOpen());
            LogicalPlan plan = emitIdentityAlias(intermediateResult, declared != null ? declared.id() : new NameId());
            return doTranslateFinal(plan, intermediateResult.kind().constant);
        }
        // Compile every branch as its own module (own step/value ids, own shifted evaluation timestamp), then link.
        var intermediateResultPlan = doTranslateUnion(
            branches.stream().map(b -> translateIntermediate(b, new NameId(), new NameId())).toList()
        );
        return doTranslateFinal(intermediateResultPlan, false);
    }

    /* Shared by every `final` translation root */
    private LogicalPlan doTranslateFinal(LogicalPlan plan, boolean localRelation) {
        plan = emitNullsFilter(cmd.source(), emitFinalProjection(plan), cmd.valueAttribute());
        return localRelation ? plan : emitByStepFilter(plan);
    }

    /**
     * A finished table exposes its series identity under the canonical {@code _timeseries} name. A packed record is
     * already canonical, so it passes through; legacy packings travel under their derived names so nodes can tell
     * them apart, and the one surviving at a root is declared as {@code _timeseries} here.
     */
    private LogicalPlan emitIdentityAlias(IntermediateResult table, NameId id) {
        if (findRecord(table.rests()) != null) {
            return table.plan();
        }
        return emitTimeSeriesAlias(table, id);
    }

    /**
     * A finished table exposes its packing under the canonical {@code _timeseries} name. Packings travel under
     * their derived names so nodes can tell them apart; the one surviving at a root is whatever the enclosing
     * regroups left, and the command declares it as {@code _timeseries}.
     */
    private LogicalPlan emitTimeSeriesAlias(IntermediateResult table, NameId id) {
        Set<String> skip = table.header().finestSkip();
        if (skip == null || skip.isEmpty()) {
            return table.plan();
        }
        Attribute packed = table.packed(skip);
        assert packed != null : "invariant: packing " + skip + " must be carried by the finished table";
        return new Eval(cmd.source(), table.plan(), List.of(new Alias(cmd.source(), MetadataAttribute.TIMESERIES, packed, id)));
    }

    /**
     * Union combinator over independently translated tabular results.
     * {@link UnionAll} aligns columns by name and null-fills missing header, then
     * {@link TopNBy} keeps single row per {@code (step, labelset)} group ordered by incoming IR order.
     */
    private LogicalPlan doTranslateUnion(List<IntermediateResult> intermediateResults) {
        // Already validated against MergePlan.MAX_BRANCHES by PromqlCommand.verify
        assert MergePlan.exceedsMaxBranches(intermediateResults.size()) == false
            : "invariant: merge branch count ["
                + intermediateResults.size()
                + "] must be less of equal MergePlan.MAX_BRANCHES ["
                + MergePlan.MAX_BRANCHES
                + "]";

        var source = cmd.source();
        var branchPlans = new ArrayList<LogicalPlan>(intermediateResults.size());
        for (int i = 0; i < intermediateResults.size(); i++) {
            // Drop null-valued rows per branch so an absent left side does not shadow a present right side.
            var ir = intermediateResults.get(i);
            LogicalPlan branchPlan = emitNullsFilter(source, emitIdentityAlias(ir, new NameId()), ir.valueColumn());
            var branchTagExpression = new Alias(source, cmd.branchColumnName(), new Literal(source, i, DataType.INTEGER));
            LogicalPlan tagged = new Eval(source, branchPlan, List.of(branchTagExpression));
            // Each branch executes as an independent sub plan whose result pages cross an exchange, and the
            // consumer assumes their layout matches output() exactly. An Eval below (e.g. the value double-cast)
            // can name-shadow an existing column: the shadowed attribute leaves output() but its channel stays
            // in the page. An explicit projection pins the page layout to the branch output (see #158164).
            branchPlans.add(new Project(source, tagged, tagged.output()));
        }

        // The attribute ids chosen here are preserved by name when the analyzer later recomputes the UnionAll output,
        // so the groupings below remain valid. The command coda projects the synthetic branch tag away.
        List<Attribute> unionOutput = VectorBinarySet.unionOutputByName(branchPlans);
        var union = new UnionAll(source, branchPlans, unionOutput);

        // Left-preferring dedup: group by every column except the value and the branch tag, keep the lowest branch.
        var groupings = new ArrayList<Expression>();
        Attribute branchAttr = null;
        for (Attribute attr : unionOutput) {
            if (attr.name().equals(cmd.branchColumnName())) {
                branchAttr = attr;
            } else if (attr.name().equals(cmd.valueColumnName()) == false) {
                groupings.add(attr);
            }
        }
        var order = new Order(source, branchAttr, Order.OrderDirection.ASC, Order.NullsPosition.LAST);
        return new TopNBy(source, union, List.of(order), new Literal(source, 1, DataType.INTEGER), groupings);
    }

    /**
     * Translates independent query fragment into intermediate result (IR).
     * Think of IR as table
     */
    private IntermediateResult translateIntermediate(LogicalPlan branch, NameId valueId) {
        IntermediateResult ir = doTranslateTryInline(translate(branch));

        var plan = ir.plan();
        var value = ir.value();
        // A vector match self-filters each operand's own source with that operand's own @timestamp; a combined outer
        // source-time filter would push one operand's @timestamp across both sources - skip over InnerJoin.
        Expression timeFilter = plan.anyMatch(p -> p instanceof InnerJoin) ? null : emitBySrcTimeFilter(branch);
        var filter = combineAndNullable(Arrays.asList(ir.pendingFilter(), timeFilter));
        if (filter != null) {
            plan = pushDownSrcTimestampFilter(plan, filter);
        }

        Set<PromotedColumn> promoted = ir.promoted();
        Set<LabelColumn> rests = ir.rests();
        if (ir.kind().constant == false) {
            // TimeSeriesAggregate always applies because InstantSelectors adds implicit last_over_time().
            // TODO: with metric references without last_over_time, a plain Aggregate could do (#141501 discussion).
            if (ir.kind().afterInitialAggregation == false) {
                IntermediateResult collapsed = collapse(ir.with(plan, ir.header(), value), ir.header(), value);
                plan = collapsed.plan();
                value = collapsed.value();
                promoted = collapsed.promoted();
                rests = collapsed.rests();
            }
            if (branch instanceof VectorBinaryComparison comparison && comparison.filterMode()) {
                VectorMatch match = comparison.match();
                if ((match.filter() != VectorMatch.Filter.NONE || match.grouping() != Joining.NONE) == false) {
                    // Filter-mode comparison (metric > x): keep the left operand's value, filter rows by the comparison.
                    // A vector-matched comparison already applied its filter inside the join translation.
                    ToDouble right = new ToDouble(comparison.right().source(), ((LiteralSelector) comparison.right()).literal());
                    var condition = comparison.op().asFunction().create(comparison.source(), value, right, configuration());
                    plan = new Filter(comparison.source(), plan, condition);
                }
            }
        }

        // The value column definition: the translateIntermediate's value expression cast to double under the caller's id.
        Alias valueAlias = emitValueDoubleCastExpression(value, valueId);
        plan = new Eval(cmd.source(), plan, List.of(valueAlias));
        if (ir.kind().constant == false) {
            plan = pushDownEvaluationTimestampFilter(plan, branch);
        }

        Kind kind = ir.kind().constant ? Kind.CONSTANT : Kind.AFTER_INITIAL_AGGREGATE;
        return new IntermediateResult(plan, ir.header(), valueAlias.toAttribute(), ir.step(), null, kind, promoted, rests);
    }

    /** Folds a branch whose value depends on nothing but the step column into a compile-time step/value relation. */
    private IntermediateResult doTranslateTryInline(IntermediateResult result) {
        Attribute stepAttr = cmd.stepAttribute();
        if (result.kind().constant
            || cmd.start().value() == null
            || findRecord(result.rests()) != null
            || result.value().references().stream().allMatch(ref -> ref.semanticEquals(stepAttr)) == false) {
            return result;
        }
        var plan = PromqlLogicalPlanBuilder.buildLocalRelation(cmd);
        var step = plan.output().getFirst();
        var value = result.value().transformUp(Attribute.class, attr -> attr.semanticEquals(stepAttr) ? step : attr);
        // The folded relation carries no label columns at all.
        return new IntermediateResult(plan, result.header(), value, step, result.pendingFilter(), Kind.CONSTANT, Set.of(), Set.of());
    }

    /**
     * Expressions compose lazily up the tree until they cross an aggregation boundary: once the plan below is
     * aggregated, the expression must materialize as the value column (an Eval) so parents reference it by attribute.
     */
    public IntermediateResult eval(IntermediateResult t, Expression value) {
        if (t.kind().afterInitialAggregation == false) {
            return t.with(t.plan(), t.header(), value);
        }
        Alias alias = new Alias(value.source(), cmd.valueColumnName(), value);
        return t.with(new Eval(cmd.source(), t.plan(), List.of(alias)), t.header(), alias.toAttribute());
    }

    /**
     * The table a {@code without} regroup exposes: the child columns surviving the dropped labels. Under a packed
     * column the labels are derived columns, so only those the enclosing translation asks for are carried; a finite
     * child keeps every remaining label because they are its label set.
     */
    public TranslationConstraint regroupWithout(TranslationConstraint child, List<String> keys) {
        TranslationConstraint header = intersect(child, keys);
        assert child.isOpen() == false || header.isOpen()
            : "invariant: required [" + required + "] must declare a packed column excluding " + keys + ", got " + child;
        return header.isOpen() ? project(header, required.labels()) : header;
    }

    /**
     * The initial aggregate: a raw table collapsed to one row per step and header column by the innermost
     * {@link TimeSeriesAggregate}, {@code function} applied in it. Passing the table's own value collapses it as is.
     */
    public IntermediateResult collapse(IntermediateResult input, TranslationConstraint header, Expression function) {
        assert input.kind().afterInitialAggregation == false : "invariant: a collapse takes a raw table";
        Alias value = new Alias(function.source(), cmd.valueColumnName(), function);
        return emitCollapse(input, header, value);
    }

    /**
     * An aggregate over a collapsed table: regrouped by {@code header} with {@code function} as the value. The regroup
     * packs its dimensions first when the header is open or the operator asks for it ({@code packed}).
     */
    public IntermediateResult regroup(IntermediateResult input, TranslationConstraint header, boolean packed, Expression function) {
        assert input.kind().afterInitialAggregation : "invariant: a regroup takes a collapsed table";
        Alias value = new Alias(function.source(), cmd.valueColumnName(), function);
        return emitRegroup(input, header, value, header.isOpen() || packed);
    }

    /** The single packed record in a rest set, or null when the table carries none (legacy or label-free). */
    public static PackedRecord findRecord(Set<LabelColumn> rests) {
        PackedRecord record = null;
        for (LabelColumn rest : rests) {
            if (rest instanceof PackedRecord packed) {
                assert record == null : "invariant: a table carries at most one packed record";
                record = packed;
            }
        }
        return record;
    }

    /** A table's labels resolved against its plan: grouping keys plus the current record, if any. */
    public record BoundLabels(LogicalPlan plan, List<Attribute> keys, PackedRecord record) {}

    /** A plan with its complete record materialized in the source relations, or a null record when skipped. */
    public record RecordPlan(LogicalPlan plan, TimeSeriesMetadataAttribute record) {}

    /**
     * The complete record for the source relations under a plan, creating it where absent. Creation is skipped
     * under joins: a record added below a join would not surface in the join output. Callers track the record
     * only when non-null.
     */
    public static RecordPlan ensureRecord(LogicalPlan plan, Source source) {
        if (plan.anyMatch(p -> p instanceof InnerJoin)) {
            return new RecordPlan(plan, null);
        }
        var found = new TimeSeriesMetadataAttribute[1];
        LogicalPlan next = plan.transformUp(EsRelation.class, relation -> {
            Attribute existing = relation.output()
                .stream()
                .filter(a -> a instanceof TimeSeriesMetadataAttribute metadata && metadata.excludedFields().isEmpty())
                .findFirst()
                .orElse(null);
            if (existing != null) {
                if (found[0] == null) {
                    found[0] = (TimeSeriesMetadataAttribute) existing;
                }
                return relation;
            }
            var created = new TimeSeriesMetadataAttribute(source, Set.of());
            if (found[0] == null) {
                found[0] = created;
            }
            return relation.withAdditionalAttributes(List.of(created));
        });
        return new RecordPlan(next, found[0]);
    }

    /**
     * Selects grouping labels from the current table. An open selection projects the current record, pushing new
     * exclusions into the loader when it is still source-backed and applying an ordinary JSON edit otherwise. A named
     * selection resolves concrete columns, null-filling labels the table does not carry.
     */
    public BoundLabels bindLabels(IntermediateResult input, TranslationConstraint header, Source source) {
        LogicalPlan plan = input.plan();
        var definitions = new ArrayList<Alias>();
        var keys = new ArrayList<Attribute>();
        PackedRecord record = findRecord(input.rests());
        if (header.isOpen() && record != null) {
            var removed = new LinkedHashSet<String>();
            for (Set<String> skip : header.skips()) {
                removed.addAll(skip);
            }
            removed.removeAll(header.labels());
            removed.removeAll(record.excluded());
            if (removed.isEmpty() == false) {
                if (record.attribute() instanceof TimeSeriesMetadataAttribute stored) {
                    // Push a direct source projection into the existing loader. Replace this record instead of adding
                    // another exclusion variant. Computed records use the ordinary JSON expression below and never
                    // read the original source again.
                    var excluded = new LinkedHashSet<>(stored.excludedFields());
                    excluded.addAll(removed);
                    Attribute projected = new TimeSeriesMetadataAttribute(source, excluded);
                    plan = plan.transformExpressionsUp(Attribute.class, a -> a.id().equals(stored.id()) ? projected : a);
                    var next = new LinkedHashSet<>(record.excluded());
                    next.addAll(removed);
                    record = new PackedRecord(projected, next);
                } else {
                    Alias updated = new Alias(source, MetadataAttribute.TIMESERIES, excludeLabels(plan, record.attribute(), removed));
                    definitions.add(updated);
                    var next = new LinkedHashSet<>(record.excluded());
                    next.addAll(removed);
                    record = new PackedRecord(updated.toAttribute(), next);
                }
            }
        }
        for (String name : header.labels()) {
            Attribute carrier = find(plan.output(), name);
            if (carrier == null) {
                // a declared label the table lacks is absent from every series: grouped under null, like Prometheus
                Alias projection = new Alias(source, name, new Literal(source, null, DataType.KEYWORD));
                definitions.add(projection);
                carrier = projection.toAttribute();
            }
            keys.add(carrier);
        }
        if (definitions.isEmpty() == false) {
            plan = new Eval(source, plan, definitions);
        }
        return new BoundLabels(plan, keys, record);
    }

    /**
     * Projects an exclusion edit over a packed record: the whole member key plus, for dotted names, the nested path,
     * addressing stored field names that themselves contain dots.
     */
    private Expression excludeLabels(LogicalPlan plan, Attribute record, Collection<String> names) {
        var fields = new LinkedHashSet<String>();
        for (String name : names) {
            fields.add(name);
            fields.add(PromqlLabels.PROMETHEUS_LABELS_PREFIX + name);
            Attribute stored = find(cmd.child().output(), name);
            if (stored instanceof FieldAttribute field) {
                fields.add(field.fieldName().string());
            }
        }
        Source source = plan.source();
        var children = new ArrayList<Expression>();
        children.add(record);
        for (String field : fields) {
            children.addAll(memberPaths(source, field));
        }
        return new JsonRemovePath(source, children);
    }

    /** Bracket-quotes one member name for a JSON path literal. */
    private static String quotedMember(String name) {
        return "[\"" + name.replace("\\", "\\\\").replace("\"", "\\\"") + "\"]";
    }

    /** Path literals addressing one packed-record field, as a whole key and — for dotted names — as a nested path. */
    private static List<Expression> memberPaths(Source source, String field) {
        if (field.indexOf('.') == -1) {
            return List.of(Literal.keyword(source, "$" + quotedMember(field)));
        }
        var nested = new StringBuilder("$");
        for (String segment : field.split("\\.", -1)) {
            nested.append(quotedMember(segment));
        }
        return List.of(Literal.keyword(source, "$" + quotedMember(field)), Literal.keyword(source, nested.toString()));
    }

    /**
     * Keeps a relabel's named projection and complete record in agreement. The destination column must already be
     * defined in the input plan; only the record edit is added here. Empty results mean removal in PromQL, expressed
     * with Case here; the JSON functions themselves retain empty strings and nulls.
     */
    public IntermediateResult withReplacedLabel(IntermediateResult input, String name, Alias destination) {
        PackedRecord record = findRecord(input.rests());
        if (record == null) {
            return input;
        }
        Source source = destination.source();
        Expression removed = excludeLabels(input.plan(), record.attribute(), List.of(name));
        Expression updated = new JsonSet(
            source,
            List.of(removed, Literal.keyword(source, "$" + quotedMember(name)), destination.toAttribute())
        );
        Expression absent = new Equals(source, destination.toAttribute(), Literal.keyword(source, ""));
        Alias edited = new Alias(source, MetadataAttribute.TIMESERIES, new Case(source, absent, List.of(removed, updated)));
        LogicalPlan plan = new Eval(source, input.plan(), List.of(edited));
        Set<LabelColumn> rests = new LinkedHashSet<>(input.rests());
        rests.remove(record);
        rests.add(new PackedRecord(edited.toAttribute(), record.excluded()));
        return input.with(plan, input.header(), input.value(), input.promoted(), Set.copyOf(rests));
    }

    /**
     * The innermost aggregate groups on the child's already-defined columns. Newer clusters group on the packed
     * record; older clusters materialize one source-backed packing per skip set in the header instead.
     */
    private IntermediateResult emitCollapse(IntermediateResult input, TranslationConstraint header, Alias value) {
        if (supportsPackedRecords()) {
            return emitPackedCollapse(input, header, value);
        }
        return emitLegacyCollapse(input, header, value);
    }

    /**
     * The packed innermost aggregate: the record (when the table carries one) groups the series under a public
     * reference rather than leaking the storage-only metadata attribute, and named labels resolve as columns.
     * No {@code TimeSeriesWithout} packing is emitted, so the lowering rule leaves the source relation alone.
     */
    private IntermediateResult emitPackedCollapse(IntermediateResult input, TranslationConstraint header, Alias value) {
        Source source = cmd.promqlPlan().source();
        Expression agg = value.child();
        // Same phase-2 guard as the legacy collapse: without any series grouping (e.g. constants like vector(5))
        // TranslateTimeSeriesAggregate passes Literals straight to phase 1.
        boolean groupsBySeries = header.isOpen() || header.labels().isEmpty() == false;
        boolean wrapWithValues = (agg instanceof AggregateFunction == false) || (agg instanceof TimeSeriesAggregateFunction);
        if (groupsBySeries && wrapWithValues) {
            value = value.replaceChild(new Values(agg.source(), agg));
        }

        if (findRecord(input.rests()) == null && header.isOpen()) {
            // The record was dropped upstream (a scalar operand's plan) or the leaf was label-free: materialize it
            // so the open collapse groups on the series identity the final projection exposes.
            RecordPlan ensured = ensureRecord(input.plan(), source);
            if (ensured.record() != null) {
                var carried = new LinkedHashSet<>(input.rests());
                carried.add(new PackedRecord(ensured.record(), Set.of()));
                input = input.with(
                    ensured.plan(),
                    input.header(),
                    input.value(),
                    IntermediateResult.retainPromoted(ensured.plan(), input.promoted()),
                    Set.copyOf(carried)
                );
            }
        }
        BoundLabels bound = bindLabels(input, header, source);
        var groupKeys = new ArrayList<NamedExpression>();
        Set<LabelColumn> rests = Set.of();
        if (bound.record() != null) {
            // Keep the public record as a reference rather than leaking the storage-only metadata attribute.
            PackedRecord record = bound.record();
            Alias alias = new Alias(source, record.attribute().name(), record.attribute(), record.attribute().id());
            groupKeys.add(alias);
            rests = Set.of(new PackedRecord(alias.toAttribute(), record.excluded()));
        }
        groupKeys.addAll(bound.keys());
        List<Attribute> output = groupKeys.stream().map(NamedExpression::toAttribute).toList();
        var collapsed = new TimeSeriesAggregate(
            source,
            bound.plan(),
            groupings(stepBucketAlias, groupKeys),
            aggregates(value, input.step(), output),
            null,
            time,
            TimeSeriesAggregate.Origin.PROMQL_COMMAND
        );
        return new IntermediateResult(
            collapsed,
            header,
            value.toAttribute(),
            input.step(),
            input.pendingFilter(),
            Kind.AFTER_INITIAL_AGGREGATE,
            bindPromoted(collapsed, header),
            rests
        );
    }

    /**
     * The innermost aggregate owns the physical {@code _timeseries} grouping and materializes every packed column in
     * the header over that column's own skip set.
     */
    private IntermediateResult emitLegacyCollapse(IntermediateResult input, TranslationConstraint header, Alias value) {
        Source source = cmd.promqlPlan().source();
        LogicalPlan plan = input.plan();
        boolean groupsBySeries = header.isOpen() || header.labels().isEmpty() == false;
        Expression agg = value.child();
        // TranslateTimeSeriesAggregate splits this node into two phases, replacing inner TimeSeriesAggregateFunctions
        // (e.g. LastOverTime) with references to phase-1 results; the phase-2 expression must remain a valid
        // AggregateFunction inside the Aggregate node:
        // Sum(LastOverTime(m)) -> Sum(ref) -- Sum survives, no wrap needed
        // LastOverTime(m) -> ref -- bare ref, needs Values(ref)
        // Mul(LastOverTime(m), 8) -> Mul(ref, 8) -- not an agg, needs Values(Mul(ref,8))
        // Guarded by groupsBySeries because without any series grouping (e.g. constants like vector(5))
        // TranslateTimeSeriesAggregate passes Literals straight to phase 1.
        boolean wrapWithValues = (agg instanceof AggregateFunction == false) || (agg instanceof TimeSeriesAggregateFunction);
        if (groupsBySeries && wrapWithValues) {
            value = value.replaceChild(new Values(agg.source(), agg));
        }

        // Every rest is materialized under its derived name, finest first, and every promoted label the relation
        // has is a key too. Every column is functionally dependent on the finest rest, so grouping by all of them
        // preserves per-series granularity while making the full header available to the surrounding query.
        var groupKeys = new ArrayList<NamedExpression>();
        var outKeys = new ArrayList<NamedExpression>();
        var rests = new ArrayList<LabelColumn>();
        for (Set<String> skip : finestFirst(header.skips())) {
            List<Expression> excluded = skip.stream().<Expression>map(label -> {
                Attribute resolved = find(plan.output(), label);
                return resolved != null ? resolved : mapToRef(label);
            }).toList();
            Alias packing = new Alias(source, mapOpen(skip), new TimeSeriesWithout(source, excluded));
            groupKeys.add(packing);
            outKeys.add(packing.toAttribute());
            // The rest reads its skip set straight from the source, so it is tracked as a source-backed column.
            rests.add(new SourcePacking(packing.toAttribute(), skip));
        }
        for (String label : header.labels()) {
            Attribute carrier = find(plan.output(), label);
            if (carrier != null) {
                groupKeys.add(carrier);
                outKeys.add(carrier);
            }
        }

        var collapsed = new TimeSeriesAggregate(
            source,
            plan,
            groupings(stepBucketAlias, groupKeys),
            aggregates(value, input.step(), outKeys),
            null,
            time,
            TimeSeriesAggregate.Origin.PROMQL_COMMAND
        );
        return new IntermediateResult(
            collapsed,
            header,
            value.toAttribute(),
            input.step(),
            input.pendingFilter(),
            Kind.AFTER_INITIAL_AGGREGATE,
            bindPromoted(collapsed, header),
            Set.copyOf(rests)
        );
    }

    /**
     * Regroups an already-aggregated table. Every regroup first resolves its physical header and null-fills missing
     * grouping columns. A packed regroup additionally packs dimensions before aggregation to prevent multi-valued
     * dimensions from splitting rows and double-counting, then unpacks them afterwards.
     */
    private IntermediateResult emitRegroup(IntermediateResult input, TranslationConstraint header, Alias value, boolean requiresPacking) {
        if (supportsPackedRecords()) {
            return emitPackedRegroup(input, header, value, requiresPacking);
        }
        return emitLegacyRegroup(input, header, value, requiresPacking);
    }

    /**
     * The packed regroup resolves its keys against the record and null-fills missing grouping columns. A packed
     * regroup additionally packs dimensions before aggregation to prevent multi-valued dimensions from splitting rows
     * and double-counting, then unpacks them afterwards; the record travels through the packing under its own id.
     */
    private IntermediateResult emitPackedRegroup(
        IntermediateResult input,
        TranslationConstraint header,
        Alias value,
        boolean requiresPacking
    ) {
        Source source = cmd.source();
        Attribute step = input.step();
        if (value.child() instanceof AggregateFunction == false) {
            value = value.replaceChild(new Values(value.child().source(), value.child()));
        }
        BoundLabels bound = bindLabels(input, header, source);
        LogicalPlan plan = bound.plan();
        var keyAttributes = new ArrayList<Attribute>(bound.keys());
        if (bound.record() != null) {
            keyAttributes.add(bound.record().attribute());
        }

        // TranslateTimeSeriesAggregate unpacks the inner TSA's dimensions and this regroup re-packs them.
        if (requiresPacking == false || keyAttributes.isEmpty()) {
            plan = new Aggregate(source, plan, groupings(step, keyAttributes), aggregates(value, step, keyAttributes));
            return regrouped(plan, input, header, value, bound.record());
        }
        Attribute packedAttribute = PackDims.newPackedAttribute(source);
        PackDims packDims = new PackDims(source, plan, keyAttributes, packedAttribute);
        Alias packedGrouping = PackDims.newPackedGrouping(source, packedAttribute);
        Aggregate agg = new Aggregate(
            source,
            packDims,
            groupings(step, List.of(packedGrouping)),
            aggregates(value, step, List.of(packedGrouping.toAttribute()))
        );
        List<Attribute> unpackedDims = keyAttributes.stream()
            .<Attribute>map(
                dim -> new ReferenceAttribute(dim.source(), null, dim.name(), dim.dataType().noText(), Nullability.TRUE, dim.id(), false)
            )
            .toList();
        UnpackDims unpackDims = new UnpackDims(source, agg, packedGrouping.toAttribute(), unpackedDims);
        List<NamedExpression> projections = new ArrayList<>(List.of(value.toAttribute(), step));
        projections.addAll(unpackedDims);
        LogicalPlan projected = new Project(source, unpackDims, projections);
        PackedRecord record = null;
        if (bound.record() != null) {
            Attribute reunpacked = null;
            for (Attribute unpacked : unpackedDims) {
                if (unpacked.id().equals(bound.record().attribute().id())) {
                    reunpacked = unpacked;
                }
            }
            assert reunpacked != null : "invariant: the packed record travels through the regroup packing";
            record = new PackedRecord(reunpacked, bound.record().excluded());
        }
        return regrouped(projected, input, header, value, record);
    }

    /**
     * The regrouped packed table binds its promoted labels from the regrouped plan and carries exactly its current
     * record; the legacy regroup instead retains whatever input rests survive in the regrouped plan.
     */
    private static IntermediateResult regrouped(
        LogicalPlan plan,
        IntermediateResult input,
        TranslationConstraint header,
        Alias value,
        PackedRecord record
    ) {
        Set<LabelColumn> rests = record == null ? IntermediateResult.retain(plan, input.rests()) : Set.of(record);
        return new IntermediateResult(
            plan,
            header,
            value.toAttribute(),
            input.step(),
            input.pendingFilter(),
            Kind.AFTER_INITIAL_AGGREGATE,
            bindPromoted(plan, header),
            rests
        );
    }

    private IntermediateResult emitLegacyRegroup(
        IntermediateResult input,
        TranslationConstraint header,
        Alias value,
        boolean requiresPacking
    ) {
        Source source = cmd.source();
        Attribute step = input.step();
        LogicalPlan plan = input.plan();
        if (value.child() instanceof AggregateFunction == false) {
            value = value.replaceChild(new Values(value.child().source(), value.child()));
        }
        List<Attribute> available = plan.output();

        var nulls = new ArrayList<Alias>();
        var keys = new ArrayList<Attribute>();
        for (Set<String> skip : finestFirst(header.skips())) {
            Attribute carrier = find(available, mapOpen(skip));
            assert carrier != null : "invariant: packing " + skip + " must be carried by the child";
            keys.add(carrier);
        }
        for (String label : header.labels()) {
            Attribute carrier = find(available, label);
            if (carrier == null) {
                // a declared label the child lacks is absent from every series: grouped under null, like Prometheus
                nulls.add(emitNullExpression(mapToRef(label)));
                carrier = nulls.getLast().toAttribute();
            }
            keys.add(carrier);
        }

        if (nulls.isEmpty() == false) {
            plan = new Eval(source, plan, nulls);
        }

        if (requiresPacking == false) {
            plan = new Aggregate(source, plan, groupings(step, keys), aggregates(value, step, keys));
            return regrouped(plan, input, header, value);
        }
        // TranslateTimeSeriesAggregate unpacks the inner TSA's dimensions and this regroup re-packs them.
        if (keys.isEmpty()) {
            plan = new Aggregate(source, plan, groupings(step, List.of()), aggregates(value, step, List.of()));
            return regrouped(plan, input, header, value);
        }
        Attribute packedAttribute = PackDims.newPackedAttribute(source);
        PackDims packDims = new PackDims(source, plan, keys, packedAttribute);
        Alias packedGrouping = PackDims.newPackedGrouping(source, packedAttribute);
        Aggregate agg = new Aggregate(
            source,
            packDims,
            groupings(step, List.of(packedGrouping)),
            aggregates(value, step, List.of(packedGrouping.toAttribute()))
        );
        List<Attribute> unpackedDims = keys.stream()
            .<Attribute>map(
                dim -> new ReferenceAttribute(dim.source(), null, dim.name(), dim.dataType().noText(), Nullability.TRUE, dim.id(), false)
            )
            .toList();
        UnpackDims unpackDims = new UnpackDims(source, agg, packedGrouping.toAttribute(), unpackedDims);
        List<NamedExpression> projections = new ArrayList<>(List.of(value.toAttribute(), step));
        projections.addAll(unpackedDims);
        return regrouped(new Project(source, unpackDims, projections), input, header, value);
    }

    /**
     * The regrouped table binds its promoted labels from the regrouped plan and keeps the input's rests that survive
     * in it. Regroups genuinely drop columns (a packed regroup projects its rests away), so survivors are retained
     * rather than propagated.
     */
    private static IntermediateResult regrouped(LogicalPlan plan, IntermediateResult input, TranslationConstraint header, Alias value) {
        return new IntermediateResult(
            plan,
            header,
            value.toAttribute(),
            input.step(),
            input.pendingFilter(),
            Kind.AFTER_INITIAL_AGGREGATE,
            bindPromoted(plan, header),
            IntermediateResult.retain(plan, input.rests())
        );
    }

    /** Projects the plan to the command's declared output, re-aliasing columns that match by name but not by id. */
    private LogicalPlan emitFinalProjection(LogicalPlan plan) {
        var lookupMap = new HashMap<String, Attribute>();
        for (var attr : plan.output()) {
            lookupMap.put(attr.name(), attr);
        }
        // Under a passthrough mapping the plan carries the concrete field (`labels.job`) while the command declares
        // the label alone, so fall back to the canonical name.
        for (var attr : plan.output()) {
            lookupMap.putIfAbsent(mapFinite(attr), attr);
        }
        var projected = new ArrayList<>(cmd.output());
        var evals = new ArrayList<Alias>();
        for (int i = 0; i < projected.size(); i++) {
            var attr = projected.get(i);
            var lookupAttr = lookupMap.get(attr.name());
            if (lookupAttr != null && lookupAttr.semanticEquals(attr) == false) {
                var alias = new Alias(lookupAttr.source(), attr.name(), lookupAttr, attr.id());
                evals.add(alias);
                projected.set(i, alias.toAttribute());
            }
        }
        if (evals.isEmpty() == false) {
            plan = new Eval(cmd.source(), plan, evals);
        }
        return new Project(cmd.source(), plan, projected);
    }

    /** Keeps only steps within the query range; step header are anchored at {@code start} and offset-independent. */
    private LogicalPlan emitByStepFilter(LogicalPlan plan) {
        var source = cmd.source();
        var step = cmd.stepAttribute();
        var start = cmd.start();
        var end = cmd.end();
        var lo = new GreaterThanOrEqual(source, step, start.value() != null ? start : Literal.dateTime(source, EPOCH_MIN));
        var hi = new LessThanOrEqual(source, step, end.value() != null ? end : Literal.dateTime(source, EPOCH_MAX));
        return new Filter(source, plan, new And(source, lo, hi));
    }

    /**
     * The source-time pushdown predicate. Expressed over the <b>raw</b> source timestamp (not the offset-shifted
     * evaluation timestamp) so it can push down to the index; the branch offset is instead folded into the bounds.
     * Expressing it over the shifted timestamp while also adjusting the bounds would apply the offset twice.
     */
    private Expression emitBySrcTimeFilter(LogicalPlan branch) {
        if (cmd.start().value() == null || cmd.end().value() == null) {
            return null;
        }
        var source = cmd.source();
        var offset = cmd.collectFirstOffsetForBranch(branch);
        var timestamp = cmd.timestamp();
        var window = cmd.sourceFilterWindow();
        var lo = new Sub(source, cmd.start(), Literal.timeDuration(source, window.plus(offset)), configuration());
        var hi = new Sub(source, cmd.end(), Literal.timeDuration(source, offset), configuration());
        return new And(source, new GreaterThanOrEqual(source, timestamp, lo), new LessThanOrEqual(source, timestamp, hi));
    }

    /** Adds an Eval on top of the source relation materializing the evaluation timestamp (@timestamp + offset). */
    private LogicalPlan pushDownEvaluationTimestampFilter(LogicalPlan plan, LogicalPlan branch) {
        if (time instanceof ReferenceAttribute ref && cmd.timestampColumnName().equals(ref.name())) {
            Expression base = cmd.timestamp();
            if (base.dataType() == DataType.DATE_NANOS) {
                base = new ToDatetime(base.source(), base, configuration());
            }
            var offset = cmd.collectFirstOffsetForBranch(branch);
            var shifted = offset.isZero() ? base : new Add(cmd.source(), base, Literal.timeDuration(cmd.source(), offset), configuration());
            var timestamp = new Alias(cmd.source(), cmd.timestampColumnName(), shifted, ref.id());
            if (supportsPackedRecords()) {
                return addEvaluationTimestamp(plan, timestamp);
            }
            return plan.transformUp(node -> node == cmd.child(), node -> new Eval(cmd.source(), node, List.of(timestamp)));
        }
        return plan;
    }

    /**
     * Defines the evaluation timestamp over the branch's source. Structural instead of identity-based: record
     * creation replaces the source relation instance during translation, so matching {@code cmd.child()} by identity
     * would miss it. Joined operands already own their evaluation times; only visit this branch's source.
     */
    private LogicalPlan addEvaluationTimestamp(LogicalPlan plan, Alias timestamp) {
        if (plan instanceof InnerJoin) {
            return plan;
        }
        if (plan instanceof EsRelation) {
            return new Eval(cmd.source(), plan, List.of(timestamp));
        }
        return plan.replaceChildren(plan.children().stream().map(child -> addEvaluationTimestamp(child, timestamp)).toList());
    }

    /** Pushes the label filter down to the EsRelation, combining with an existing relation filter. */
    private LogicalPlan pushDownSrcTimestampFilter(LogicalPlan plan, Expression filterCondition) {
        return plan.transformUp(LogicalPlan.class, p -> {
            if (p instanceof Filter f && f.child() instanceof EsRelation) {
                return new Filter(f.source(), f.child(), new And(f.source(), f.condition(), filterCondition));
            } else if (p instanceof EsRelation) {
                return new Filter(cmd.source(), p, filterCondition);
            }
            return p;
        });
    }

    /** The value column definition: the translateIntermediate's value expression, cast to double unless it provably is one. */
    private Alias emitValueDoubleCastExpression(Expression valueExpr, NameId valueId) {
        if ((valueExpr instanceof Attribute == false && valueExpr.resolved() && valueExpr.dataType() == DataType.DOUBLE) == false) {
            valueExpr = new ToDouble(cmd.source(), valueExpr);
        }
        return new Alias(cmd.source(), cmd.valueColumnName(), valueExpr, valueId);
    }

    /**
     * The {@code step} bucket for a branch: the {@link TStep} grouping key shared across all aggregation groupings,
     * derived from the (possibly offset-shifted) evaluation timestamp - so an {@code offset} shifts which samples
     * fall into each fixed output bucket without moving the buckets. {@code stepId} names the synthetic column.
     */
    private Alias emitStepBucketExpression(NameId stepId, Expression time) {
        Expression size;
        Expression start;
        Expression end;
        if (cmd.isInstantQuery()) {
            size = Literal.timeDuration(cmd.source(), cmd.resolveInstantQueryWindow());
            start = new Sub(cmd.source(), cmd.start(), size, configuration());
            end = cmd.end();
        } else {
            size = cmd.resolveTimeBucketSize();
            start = cmd.start().value() != null ? cmd.start() : Literal.dateTime(cmd.source(), EPOCH_MIN);
            end = cmd.end().value() != null ? cmd.end() : Literal.dateTime(cmd.source(), EPOCH_MAX);
        }
        var tstep = new TStep(size.source(), size, start, end, time, configuration());
        return new Alias(tstep.source(), cmd.stepColumnName(), tstep, stepId);
    }

    private boolean canCreateStepBucket() {
        if (cmd.timestamp() == null || cmd.timestamp().resolved() == false) {
            return cmd.isRangeQuery() == false || cmd.buckets() == null || cmd.buckets().value() == null;
        }
        return true;
    }

    private static List<Expression> groupings(Expression step, List<? extends NamedExpression> keys) {
        var groupings = new ArrayList<Expression>(keys.size() + 1);
        groupings.add(step);
        groupings.addAll(keys);
        return groupings;
    }

    private static List<NamedExpression> aggregates(NamedExpression value, Attribute step, List<? extends NamedExpression> keys) {
        var aggregates = new ArrayList<NamedExpression>(keys.size() + 2);
        aggregates.add(value);
        aggregates.add(step);
        aggregates.addAll(keys);
        return aggregates;
    }

    /** Flattens a left-associative top-level {@code or} chain into branches; branch 0 has the highest precedence. */
    private static void flattenUnion(LogicalPlan node, List<LogicalPlan> branches) {
        if (node instanceof VectorBinarySet setOp && setOp.op() == VectorBinarySet.SetOp.UNION) {
            flattenUnion(setOp.left(), branches);
            flattenUnion(setOp.right(), branches);
        } else {
            branches.add(node);
        }
    }

    /** PromQL drops series with missing data: filter out rows whose value is null (null label columns are valid). */
    private static LogicalPlan emitNullsFilter(Source source, LogicalPlan plan, Attribute value) {
        return new Filter(source, plan, new IsNotNull(value.source(), value));
    }

    // -- core --

    /**
     * The single value flowing through the compiler: a table - an ESQL plan together with its defined columns. The
     * {@link TranslationConstraint} names the label columns and the plan carries them; value and step are the two
     * columns every table has. Every AST node translates to one and the stitching operations (joins, unions,
     * regroups, the command coda) compose them by their declared columns. Mid-descent the value is a (possibly not
     * yet materialized) expression parents compose into larger expressions; a finished table's value is a defined
     * column ({@link #valueColumn()}).
     * <p>
     * Labels use a dual representation like ClickHouse: promoted labels carried directly plus {@code rest} columns
     * holding every remaining label as a packed/dynamic dictionary. A {@code rest} may overlap the promoted names.
     */
    public record IntermediateResult(
        /* Output ESQL plan: the source relation (cmd.child()) with this node's operators stacked on top. */
        LogicalPlan plan,
        /* The label columns this subtree exposes; the plan carries them under their canonical or derived names. */
        TranslationConstraint header,
        /* This node's numeric value: an expression mid-descent, a defined column once aggregated. */
        Expression value,
        /* The step column. */
        Attribute step,
        /* Label matcher predicate; flows up until pushed to the relation or folded into an aggregate filter. */
        Expression pendingFilter,
        /* The translator tracks what it built instead of inspecting the plan. */
        Kind kind,
        /* Promoted labels carried directly, in header order. */
        Set<PromotedColumn> promoted,
        /* Rests: packed/dynamic dictionaries of the remaining labels, one per exclusion set. */
        Set<LabelColumn> rests
    ) {
        /** The lifecycle of an intermediate result. A constant is always a finished (aggregation-free) local relation. */
        public enum Kind {
            BEFORE_INITIAL_AGGREGATE(false, false),
            AFTER_INITIAL_AGGREGATE(true, false),
            CONSTANT(true, true);

            /** A local relation needs no source filtering or aggregation. */
            public final boolean constant;
            /** Value expressions above this boundary must materialize before being consumed. */
            public final boolean afterInitialAggregation;

            Kind(boolean afterInitialAggregation, boolean constant) {
                this.afterInitialAggregation = afterInitialAggregation;
                this.constant = constant;
            }
        }

        public IntermediateResult {
            promoted = Collections.unmodifiableSet(new LinkedHashSet<>(promoted));
            rests = Set.copyOf(rests);
            assert promoted.stream().map(PromotedColumn::attribute).allMatch(a -> plan.outputSet().contains(a))
                : "every promoted label must belong to the plan output";
            assert promoted.stream().map(PromotedColumn::name).distinct().count() == promoted.size()
                : "at most one promoted column per name";
            assert rests.stream().map(LabelColumn::attribute).allMatch(a -> plan.outputSet().contains(a))
                : "every rest column must belong to the plan output";
            // No disjointness check: a rest may overlap the promoted names.
            // Singleton enforcement: a table with the packed record carries no source packings. Older clusters
            // never create the record (they stay on one rest per exclusion set); newer clusters never emit
            // packings, so the record is always the only rest.
            assert findRecord(rests) == null || rests.stream().noneMatch(SourcePacking.class::isInstance)
                : "the packed record never mixes with source packings";
        }

        /** A raw input whose value may still contain per-series aggregate expressions. */
        public IntermediateResult(LogicalPlan plan, TranslationConstraint header, Expression value, Attribute step) {
            this(plan, header, value, step, null, Kind.BEFORE_INITIAL_AGGREGATE, Set.of(), Set.of());
        }

        /** A raw input carrying a selector predicate until source filtering or aggregate assembly consumes it. */
        public IntermediateResult(
            LogicalPlan plan,
            TranslationConstraint header,
            Expression value,
            Attribute step,
            Expression selectorFilter
        ) {
            this(plan, header, value, step, selectorFilter, Kind.BEFORE_INITIAL_AGGREGATE, Set.of(), Set.of());
        }

        /** A raw input carrying a selector predicate and its dual label columns (promoted plus rests). */
        public IntermediateResult(
            LogicalPlan plan,
            TranslationConstraint header,
            Expression value,
            Attribute step,
            Expression selectorFilter,
            Set<PromotedColumn> promoted,
            Set<LabelColumn> rests
        ) {
            this(plan, header, value, step, selectorFilter, Kind.BEFORE_INITIAL_AGGREGATE, promoted, rests);
        }

        /** This table rebuilt around a new plan, header and value, keeping its other properties. */
        public IntermediateResult with(LogicalPlan plan, TranslationConstraint header, Expression value) {
            return new IntermediateResult(plan, header, value, step, pendingFilter, kind, promoted, rests);
        }

        /**
         * Keeps the tracked rests still present in a merged plan. Merges (binary operators, joins) genuinely drop
         * some inputs' materialized columns, so merge sites retain rather than propagate blindly; producing sites must
         * still account for every column exactly, which the constructor asserts.
         */
        public static Set<LabelColumn> retain(LogicalPlan plan, Set<LabelColumn> rests) {
            Set<Attribute> output = Set.copyOf(plan.output());
            return rests.stream().filter(column -> output.contains(column.attribute())).collect(Collectors.toUnmodifiableSet());
        }

        /**
         * Keeps the promoted labels still present in a merged plan, in declaration order. Like {@link #retain},
         * merges drop inputs' columns, so merge sites retain rather than propagate blindly.
         */
        public static Set<PromotedColumn> retainPromoted(LogicalPlan plan, Set<PromotedColumn> promoted) {
            Set<Attribute> output = Set.copyOf(plan.output());
            var retained = new LinkedHashSet<PromotedColumn>();
            for (PromotedColumn column : promoted) {
                if (output.contains(column.attribute())) {
                    retained.add(column);
                }
            }
            return Collections.unmodifiableSet(retained);
        }

        /** This table rebuilt with a new dual label assignment (promoted plus rests). */
        public IntermediateResult with(
            LogicalPlan plan,
            TranslationConstraint header,
            Expression value,
            Set<PromotedColumn> promoted,
            Set<LabelColumn> rests
        ) {
            return new IntermediateResult(plan, header, value, step, pendingFilter, kind, promoted, rests);
        }

        /** The value as a defined column; only valid on a finished table. */
        public Attribute valueColumn() {
            return (Attribute) value;
        }

        /** The attribute carrying a promoted label, or null when the table lacks it. */
        public Attribute label(String name) {
            for (PromotedColumn column : promoted) {
                if (column.name().equals(name)) {
                    return column.attribute();
                }
            }
            return find(plan.output(), name);
        }

        /** The attribute carrying a rest with this exclusion set, or null when the table lacks it. */
        public Attribute packed(Set<String> skip) {
            for (LabelColumn rest : rests) {
                if (rest.excluded().equals(skip)) {
                    return rest.attribute();
                }
            }
            return find(plan.output(), mapOpen(skip));
        }
    }

    // -- helpers --

    /** The canonical name exposed at a finished command's boundary. */
    public static String mapOpen() {
        return mapOpen(Set.of());
    }

    /** The existing internal name distinguishing packings with different exclusions. */
    public static String mapOpen(Set<String> skip) {
        return MetadataAttribute.TIMESERIES + (skip.isEmpty() ? "" : "$" + String.join("$", new TreeSet<>(skip)));
    }

    /** Canonical label names in declaration order, without duplicates. */
    public static List<String> mapFinite(Collection<? extends Attribute> attributes) {
        return attributes.stream().map(TranslationContext::mapFinite).distinct().toList();
    }

    /** Label names ignore the physical field prefix used for Prometheus passthrough dimensions. */
    public static String mapFinite(Attribute attribute) {
        String name = attribute instanceof FieldAttribute field ? field.fieldName().string() : attribute.name();
        return name.startsWith(PROMETHEUS_LABELS_PREFIX) ? name.substring(PROMETHEUS_LABELS_PREFIX.length()) : name;
    }

    /** A missing label's reference, subsequently defined as null by its consumer. */
    public static Attribute mapToRef(String name) {
        return new ReferenceAttribute(Source.EMPTY, name, DataType.KEYWORD);
    }

    /** The skip sets of a header ordered finest first: the grain-fixing packing leads, coarser variants follow. */
    public static List<Set<String>> finestFirst(Set<Set<String>> skips) {
        return skips.stream().sorted(Comparator.comparingInt(Set::size)).toList();
    }

    /** A null-valued column under the attribute's own name and id, typed like the attribute (keyword when unresolved). */
    public static Alias emitNullExpression(Attribute attribute) {
        var nullLiteral = new Literal(attribute.source(), null, attribute.resolved() ? attribute.dataType() : DataType.KEYWORD);
        return new Alias(attribute.source(), attribute.name(), nullLiteral, attribute.id());
    }

    /** Finds a canonical label, preferring its backing field to a same-named bare reference. */
    public static Attribute find(List<Attribute> attributes, String label) {
        Attribute bareMatch = null;
        for (Attribute attribute : attributes) {
            if (mapFinite(attribute).equals(label)) {
                if (attribute.name().equals(label) == false) {
                    return attribute;
                }
                bareMatch = attribute;
            }
        }
        return bareMatch;
    }

    /**
     * Binds the promoted labels of a header to the plan columns carrying them, in header order. Labels the plan
     * lacks are omitted; consumers null-fill them. A rest may still carry an omitted or bound label.
     */
    public static Set<PromotedColumn> bindPromoted(LogicalPlan plan, TranslationConstraint header) {
        var bound = new LinkedHashSet<PromotedColumn>();
        List<Attribute> output = plan.output();
        for (String name : header.labels()) {
            Attribute carrier = find(output, name);
            if (carrier != null) {
                bound.add(new PromotedColumn(name, carrier));
            }
        }
        return Collections.unmodifiableSet(bound);
    }
}
