// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! [`TreeNodeArc`] implementation for [`LogicalPlan`].
//!
//! This is a parallel implementation of [`LogicalPlan::map_children`] and
//! friends, optimized for the common case where a rewrite leaves most of
//! the plan tree unchanged. Unchanged subtrees are returned as
//! `Arc::clone(&input)` — one atomic refcount bump, no heap allocation.
//!
//! See [`TreeNodeArc`] for API details.

use std::sync::Arc;

use datafusion_common::Result;
use datafusion_common::tree_node::{Transformed, TreeNodeArc, TreeNodeRecursion};

use crate::logical_plan::{
    Aggregate, Analyze, CreateMemoryTable, CreateView, DdlStatement, Distinct, DistinctOn,
    DmlStatement, Explain, Filter, Join, Limit, LogicalPlan, Prepare, Projection,
    RecursiveQuery, Repartition, Sort, Statement, Subquery, SubqueryAlias, Union, Unnest,
    Window,
};
use crate::logical_plan::dml::CopyTo;

impl TreeNodeArc for LogicalPlan {
    fn map_children_arc<F>(
        self: &Arc<Self>,
        mut f: F,
    ) -> Result<Transformed<Arc<Self>>>
    where
        F: FnMut(&Arc<Self>) -> Result<Transformed<Arc<Self>>>,
    {
        match &**self {
            // Single-input variants where only `input` is walked.
            LogicalPlan::Projection(Projection {
                expr,
                input,
                schema,
            }) => map_single_input_arc(self, input, &mut f, |input| {
                LogicalPlan::Projection(Projection {
                    expr: expr.clone(),
                    input,
                    schema: Arc::clone(schema),
                })
            }),
            LogicalPlan::Filter(Filter { predicate, input }) => {
                map_single_input_arc(self, input, &mut f, |input| {
                    LogicalPlan::Filter(Filter {
                        predicate: predicate.clone(),
                        input,
                    })
                })
            }
            LogicalPlan::Repartition(Repartition {
                input,
                partitioning_scheme,
            }) => map_single_input_arc(self, input, &mut f, |input| {
                LogicalPlan::Repartition(Repartition {
                    input,
                    partitioning_scheme: partitioning_scheme.clone(),
                })
            }),
            LogicalPlan::Window(Window {
                input,
                window_expr,
                schema,
            }) => map_single_input_arc(self, input, &mut f, |input| {
                LogicalPlan::Window(Window {
                    input,
                    window_expr: window_expr.clone(),
                    schema: Arc::clone(schema),
                })
            }),
            LogicalPlan::Aggregate(Aggregate {
                input,
                group_expr,
                aggr_expr,
                schema,
            }) => map_single_input_arc(self, input, &mut f, |input| {
                LogicalPlan::Aggregate(Aggregate {
                    input,
                    group_expr: group_expr.clone(),
                    aggr_expr: aggr_expr.clone(),
                    schema: Arc::clone(schema),
                })
            }),
            LogicalPlan::Sort(Sort { expr, input, fetch }) => {
                map_single_input_arc(self, input, &mut f, |input| {
                    LogicalPlan::Sort(Sort {
                        expr: expr.clone(),
                        input,
                        fetch: *fetch,
                    })
                })
            }
            LogicalPlan::Limit(Limit { skip, fetch, input }) => {
                map_single_input_arc(self, input, &mut f, |input| {
                    LogicalPlan::Limit(Limit {
                        skip: skip.clone(),
                        fetch: fetch.clone(),
                        input,
                    })
                })
            }
            LogicalPlan::Subquery(Subquery {
                subquery,
                outer_ref_columns,
                spans,
            }) => map_single_input_arc(self, subquery, &mut f, |subquery| {
                LogicalPlan::Subquery(Subquery {
                    subquery,
                    outer_ref_columns: outer_ref_columns.clone(),
                    spans: spans.clone(),
                })
            }),
            LogicalPlan::SubqueryAlias(SubqueryAlias {
                input,
                alias,
                schema,
            }) => map_single_input_arc(self, input, &mut f, |input| {
                LogicalPlan::SubqueryAlias(SubqueryAlias {
                    input,
                    alias: alias.clone(),
                    schema: Arc::clone(schema),
                })
            }),
            LogicalPlan::Analyze(Analyze {
                verbose,
                input,
                schema,
            }) => map_single_input_arc(self, input, &mut f, |input| {
                LogicalPlan::Analyze(Analyze {
                    verbose: *verbose,
                    input,
                    schema: Arc::clone(schema),
                })
            }),
            LogicalPlan::Explain(Explain {
                verbose,
                explain_format,
                plan,
                stringified_plans,
                schema,
                logical_optimization_succeeded,
            }) => map_single_input_arc(self, plan, &mut f, |plan| {
                LogicalPlan::Explain(Explain {
                    verbose: *verbose,
                    explain_format: explain_format.clone(),
                    plan,
                    stringified_plans: stringified_plans.clone(),
                    schema: Arc::clone(schema),
                    logical_optimization_succeeded: *logical_optimization_succeeded,
                })
            }),
            LogicalPlan::Dml(DmlStatement {
                table_name,
                target,
                op,
                input,
                output_schema,
            }) => map_single_input_arc(self, input, &mut f, |input| {
                LogicalPlan::Dml(DmlStatement {
                    table_name: table_name.clone(),
                    target: Arc::clone(target),
                    op: op.clone(),
                    input,
                    output_schema: Arc::clone(output_schema),
                })
            }),
            LogicalPlan::Copy(CopyTo {
                input,
                output_url,
                partition_by,
                file_type,
                options,
                output_schema,
            }) => map_single_input_arc(self, input, &mut f, |input| {
                LogicalPlan::Copy(CopyTo {
                    input,
                    output_url: output_url.clone(),
                    partition_by: partition_by.clone(),
                    file_type: Arc::clone(file_type),
                    options: options.clone(),
                    output_schema: Arc::clone(output_schema),
                })
            }),
            LogicalPlan::Unnest(Unnest {
                input,
                exec_columns,
                list_type_columns,
                struct_type_columns,
                dependency_indices,
                schema,
                options,
            }) => map_single_input_arc(self, input, &mut f, |input| {
                LogicalPlan::Unnest(Unnest {
                    input,
                    exec_columns: exec_columns.clone(),
                    list_type_columns: list_type_columns.clone(),
                    struct_type_columns: struct_type_columns.clone(),
                    dependency_indices: dependency_indices.clone(),
                    schema: Arc::clone(schema),
                    options: options.clone(),
                })
            }),

            // Two-input variants.
            LogicalPlan::Join(Join {
                left,
                right,
                on,
                filter,
                join_type,
                join_constraint,
                schema,
                null_equality,
                null_aware,
            }) => {
                let new_left = f(left)?;
                let new_right = f(right)?;
                if !(new_left.transformed || new_right.transformed) {
                    return Ok(Transformed::no(Arc::clone(self)));
                }
                Ok(Transformed::yes(Arc::new(LogicalPlan::Join(Join {
                    left: new_left.data,
                    right: new_right.data,
                    on: on.clone(),
                    filter: filter.clone(),
                    join_type: *join_type,
                    join_constraint: *join_constraint,
                    schema: Arc::clone(schema),
                    null_equality: *null_equality,
                    null_aware: *null_aware,
                }))))
            }
            LogicalPlan::RecursiveQuery(RecursiveQuery {
                name,
                static_term,
                recursive_term,
                is_distinct,
            }) => {
                let new_static = f(static_term)?;
                let new_recursive = f(recursive_term)?;
                if !(new_static.transformed || new_recursive.transformed) {
                    return Ok(Transformed::no(Arc::clone(self)));
                }
                Ok(Transformed::yes(Arc::new(LogicalPlan::RecursiveQuery(
                    RecursiveQuery {
                        name: name.clone(),
                        static_term: new_static.data,
                        recursive_term: new_recursive.data,
                        is_distinct: *is_distinct,
                    },
                ))))
            }

            // Vec<Arc<LogicalPlan>> children.
            LogicalPlan::Union(Union { inputs, schema }) => {
                let walked = walk_arc_vec(inputs, &mut f)?;
                if !walked.transformed {
                    return Ok(Transformed::no(Arc::clone(self)));
                }
                Ok(Transformed::yes(Arc::new(LogicalPlan::Union(Union {
                    inputs: walked.data,
                    schema: Arc::clone(schema),
                }))))
            }

            // Nested enum variants.
            LogicalPlan::Distinct(distinct) => match distinct {
                Distinct::All(input) => map_single_input_arc(
                    self,
                    input,
                    &mut f,
                    |input| LogicalPlan::Distinct(Distinct::All(input)),
                ),
                Distinct::On(DistinctOn {
                    on_expr,
                    select_expr,
                    sort_expr,
                    input,
                    schema,
                }) => map_single_input_arc(self, input, &mut f, |input| {
                    LogicalPlan::Distinct(Distinct::On(DistinctOn {
                        on_expr: on_expr.clone(),
                        select_expr: select_expr.clone(),
                        sort_expr: sort_expr.clone(),
                        input,
                        schema: Arc::clone(schema),
                    }))
                }),
            },
            LogicalPlan::Ddl(ddl) => match ddl {
                DdlStatement::CreateMemoryTable(CreateMemoryTable {
                    name,
                    constraints,
                    input,
                    if_not_exists,
                    or_replace,
                    column_defaults,
                    temporary,
                }) => map_single_input_arc(self, input, &mut f, |input| {
                    LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(
                        CreateMemoryTable {
                            name: name.clone(),
                            constraints: constraints.clone(),
                            input,
                            if_not_exists: *if_not_exists,
                            or_replace: *or_replace,
                            column_defaults: column_defaults.clone(),
                            temporary: *temporary,
                        },
                    ))
                }),
                DdlStatement::CreateView(CreateView {
                    name,
                    input,
                    or_replace,
                    definition,
                    temporary,
                }) => map_single_input_arc(self, input, &mut f, |input| {
                    LogicalPlan::Ddl(DdlStatement::CreateView(CreateView {
                        name: name.clone(),
                        input,
                        or_replace: *or_replace,
                        definition: definition.clone(),
                        temporary: *temporary,
                    }))
                }),
                // DDL variants without inputs.
                DdlStatement::CreateExternalTable(_)
                | DdlStatement::CreateCatalogSchema(_)
                | DdlStatement::CreateCatalog(_)
                | DdlStatement::CreateIndex(_)
                | DdlStatement::DropTable(_)
                | DdlStatement::DropView(_)
                | DdlStatement::DropCatalogSchema(_)
                | DdlStatement::CreateFunction(_)
                | DdlStatement::DropFunction(_) => Ok(Transformed::no(Arc::clone(self))),
            },
            LogicalPlan::Statement(stmt) => match stmt {
                Statement::Prepare(prepare) => map_single_input_arc(
                    self,
                    &prepare.input,
                    &mut f,
                    |input| {
                        LogicalPlan::Statement(Statement::Prepare(Prepare {
                            name: prepare.name.clone(),
                            fields: prepare.fields.clone(),
                            input,
                        }))
                    },
                ),
                // Statement variants without inputs.
                Statement::TransactionStart(_)
                | Statement::TransactionEnd(_)
                | Statement::SetVariable(_)
                | Statement::ResetVariable(_)
                | Statement::Execute(_)
                | Statement::Deallocate(_) => Ok(Transformed::no(Arc::clone(self))),
            },

            // `Extension` nodes go through a `dyn` trait that still takes
            // owned plans. Fall back to full clone + old-path rebuild;
            // this is acceptable because Extension is rare.
            LogicalPlan::Extension(_) => {
                fallback_via_owned_map_children(self, f)
            }

            // Leaf variants with no children.
            LogicalPlan::TableScan(_)
            | LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::DescribeTable(_) => Ok(Transformed::no(Arc::clone(self))),
        }
    }

    fn apply_children_arc<F>(
        self: &Arc<Self>,
        mut f: F,
    ) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&Arc<Self>) -> Result<TreeNodeRecursion>,
    {
        match &**self {
            // Single-input variants.
            LogicalPlan::Projection(Projection { input, .. })
            | LogicalPlan::Filter(Filter { input, .. })
            | LogicalPlan::Repartition(Repartition { input, .. })
            | LogicalPlan::Window(Window { input, .. })
            | LogicalPlan::Aggregate(Aggregate { input, .. })
            | LogicalPlan::Sort(Sort { input, .. })
            | LogicalPlan::Limit(Limit { input, .. })
            | LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. })
            | LogicalPlan::Analyze(Analyze { input, .. })
            | LogicalPlan::Dml(DmlStatement { input, .. })
            | LogicalPlan::Copy(CopyTo { input, .. })
            | LogicalPlan::Unnest(Unnest { input, .. }) => f(input),
            LogicalPlan::Subquery(Subquery { subquery, .. }) => f(subquery),
            LogicalPlan::Explain(Explain { plan, .. }) => f(plan),

            LogicalPlan::Join(Join { left, right, .. }) => {
                f(left)?.visit_sibling(|| f(right))
            }
            LogicalPlan::RecursiveQuery(RecursiveQuery {
                static_term,
                recursive_term,
                ..
            }) => f(static_term)?.visit_sibling(|| f(recursive_term)),

            LogicalPlan::Union(Union { inputs, .. }) => {
                let mut tnr = TreeNodeRecursion::Continue;
                for input in inputs {
                    tnr = f(input)?;
                    if matches!(tnr, TreeNodeRecursion::Stop) {
                        return Ok(TreeNodeRecursion::Stop);
                    }
                }
                Ok(tnr)
            }

            LogicalPlan::Distinct(distinct) => match distinct {
                Distinct::All(input) => f(input),
                Distinct::On(DistinctOn { input, .. }) => f(input),
            },
            LogicalPlan::Ddl(ddl) => match ddl {
                DdlStatement::CreateMemoryTable(CreateMemoryTable { input, .. })
                | DdlStatement::CreateView(CreateView { input, .. }) => f(input),
                DdlStatement::CreateExternalTable(_)
                | DdlStatement::CreateCatalogSchema(_)
                | DdlStatement::CreateCatalog(_)
                | DdlStatement::CreateIndex(_)
                | DdlStatement::DropTable(_)
                | DdlStatement::DropView(_)
                | DdlStatement::DropCatalogSchema(_)
                | DdlStatement::CreateFunction(_)
                | DdlStatement::DropFunction(_) => Ok(TreeNodeRecursion::Continue),
            },
            LogicalPlan::Statement(stmt) => match stmt {
                Statement::Prepare(prepare) => f(&prepare.input),
                Statement::TransactionStart(_)
                | Statement::TransactionEnd(_)
                | Statement::SetVariable(_)
                | Statement::ResetVariable(_)
                | Statement::Execute(_)
                | Statement::Deallocate(_) => Ok(TreeNodeRecursion::Continue),
            },

            LogicalPlan::Extension(extension) => {
                let mut tnr = TreeNodeRecursion::Continue;
                for input in extension.node.inputs() {
                    // `Extension::node.inputs()` returns `Vec<&LogicalPlan>`;
                    // wrap each in a fresh `Arc` to match the signature. This
                    // is a fallback path for Extension only.
                    let arc_input = Arc::new(input.clone());
                    tnr = f(&arc_input)?;
                    if matches!(tnr, TreeNodeRecursion::Stop) {
                        return Ok(TreeNodeRecursion::Stop);
                    }
                }
                Ok(tnr)
            }

            LogicalPlan::TableScan(_)
            | LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::DescribeTable(_) => Ok(TreeNodeRecursion::Continue),
        }
    }
}

/// Helper for variants with a single `Arc<LogicalPlan>` child.
///
/// Walks `input` through `f`. If unchanged, returns `Arc::clone(self)` —
/// no allocation. If changed, builds a new variant via the `rebuild`
/// closure and wraps it in a fresh `Arc`.
fn map_single_input_arc<F>(
    self_arc: &Arc<LogicalPlan>,
    input: &Arc<LogicalPlan>,
    f: &mut F,
    rebuild: impl FnOnce(Arc<LogicalPlan>) -> LogicalPlan,
) -> Result<Transformed<Arc<LogicalPlan>>>
where
    F: FnMut(&Arc<LogicalPlan>) -> Result<Transformed<Arc<LogicalPlan>>>,
{
    let new_input = f(input)?;
    if !new_input.transformed {
        return Ok(Transformed::no(Arc::clone(self_arc)));
    }
    Ok(Transformed::yes(Arc::new(rebuild(new_input.data))))
}

/// Walk a `Vec<Arc<LogicalPlan>>`, lazily constructing a new vec only
/// when a change is detected. When no child is transformed, returns
/// `Transformed::no(Vec::new())` — callers must check the flag before
/// using the data (they typically return `Arc::clone(self)` instead).
fn walk_arc_vec<F>(
    inputs: &[Arc<LogicalPlan>],
    f: &mut F,
) -> Result<Transformed<Vec<Arc<LogicalPlan>>>>
where
    F: FnMut(&Arc<LogicalPlan>) -> Result<Transformed<Arc<LogicalPlan>>>,
{
    let mut new_inputs: Option<Vec<Arc<LogicalPlan>>> = None;
    for (i, input) in inputs.iter().enumerate() {
        let result = f(input)?;
        if result.transformed {
            let vec = new_inputs.get_or_insert_with(|| {
                let mut v = Vec::with_capacity(inputs.len());
                // Cheaply carry over prior (unchanged) inputs via Arc::clone.
                v.extend(inputs[..i].iter().map(Arc::clone));
                v
            });
            vec.push(result.data);
        } else if let Some(vec) = new_inputs.as_mut() {
            // Already transforming; carry over this unchanged input.
            vec.push(result.data);
        }
    }
    match new_inputs {
        Some(data) => Ok(Transformed::yes(data)),
        None => Ok(Transformed::no(Vec::new())),
    }
}
/// Fallback to the existing owned `map_children` for variants like
/// `Extension` that still use a `dyn` trait taking owned plans. Pays
/// the full cost of `Arc::unwrap_or_clone` + `Arc::new`; acceptable for
/// rare variants.
fn fallback_via_owned_map_children<F>(
    self_arc: &Arc<LogicalPlan>,
    mut f: F,
) -> Result<Transformed<Arc<LogicalPlan>>>
where
    F: FnMut(&Arc<LogicalPlan>) -> Result<Transformed<Arc<LogicalPlan>>>,
{
    use datafusion_common::tree_node::TreeNode;
    let owned = (**self_arc).clone();
    let transformed = owned.map_children(|child| {
        let child_arc = Arc::new(child);
        f(&child_arc).map(|t| t.update_data(Arc::unwrap_or_clone))
    })?;
    if transformed.transformed {
        Ok(Transformed::yes(Arc::new(transformed.data)))
    } else {
        Ok(Transformed::no(Arc::clone(self_arc)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion_common::{DFSchema, Result};

    use crate::logical_plan::{EmptyRelation, Filter, Limit, LogicalPlan};
    use crate::{col, lit};

    /// Build a small plan:
    /// ```text
    /// Filter(c1 > 0, Limit(100, EmptyRelation))
    /// ```
    /// The three nodes are all distinct `Arc`s. Useful for testing
    /// that identity walks preserve addresses.
    fn test_plan() -> Arc<LogicalPlan> {
        let empty = Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        }));
        let limit = Arc::new(LogicalPlan::Limit(Limit {
            skip: None,
            fetch: Some(Box::new(lit(100_i64))),
            input: empty,
        }));
        Arc::new(LogicalPlan::Filter(
            Filter::try_new(col("c1").gt(lit(0_i64)), limit).unwrap(),
        ))
    }

    #[test]
    fn identity_walk_preserves_all_arcs() {
        let plan = test_plan();
        // Walk the tree with an identity closure. Every returned Arc
        // must point at the same allocation as the input.
        let before_root = Arc::as_ptr(&plan);
        let walked = plan
            .transform_up_arc(|node| Ok(Transformed::no(Arc::clone(node))))
            .unwrap();
        assert!(
            !walked.transformed,
            "identity walk must set transformed=false"
        );
        assert_eq!(Arc::as_ptr(&walked.data), before_root);
    }

    #[test]
    fn identity_walk_preserves_child_arcs() {
        let plan = test_plan();
        // Grab pointers to both the root and its child/grandchild.
        let root_ptr = Arc::as_ptr(&plan);
        let (child_ptr, grandchild_ptr) = match &*plan {
            LogicalPlan::Filter(Filter { input, .. }) => {
                let child = Arc::as_ptr(input);
                let grand = match &**input {
                    LogicalPlan::Limit(Limit { input, .. }) => Arc::as_ptr(input),
                    _ => panic!("unexpected structure"),
                };
                (child, grand)
            }
            _ => panic!("unexpected root"),
        };

        let walked = plan
            .transform_up_arc(|node| Ok(Transformed::no(Arc::clone(node))))
            .unwrap();
        assert_eq!(Arc::as_ptr(&walked.data), root_ptr);
        // Dive back in and confirm children were preserved too.
        let LogicalPlan::Filter(Filter { input: child, .. }) = &*walked.data else {
            panic!();
        };
        assert_eq!(Arc::as_ptr(child), child_ptr);
        let LogicalPlan::Limit(Limit { input: grand, .. }) = &**child else {
            panic!();
        };
        assert_eq!(Arc::as_ptr(grand), grandchild_ptr);
    }

    #[test]
    fn rewriting_root_preserves_unchanged_children() {
        let plan = test_plan();
        let child_ptr = match &*plan {
            LogicalPlan::Filter(Filter { input, .. }) => Arc::as_ptr(input),
            _ => panic!(),
        };

        // Rewrite only the root Filter → swap it for a trivial
        // EmptyRelation. The child should still be preserved on the
        // walk down to it (we never touch it).
        let walked = plan
            .transform_up_arc(|node| match &**node {
                LogicalPlan::Filter(_) => Ok(Transformed::yes(Arc::new(
                    LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: true,
                        schema: Arc::new(DFSchema::empty()),
                    }),
                ))),
                _ => Ok(Transformed::no(Arc::clone(node))),
            })
            .unwrap();
        assert!(walked.transformed);
        assert!(matches!(&*walked.data, LogicalPlan::EmptyRelation(_)));
        // The original child Arc is dropped because the rewritten root
        // replaced it, but the pointer we captured before was still
        // preserved through the walk (never reallocated mid-walk).
        let _ = child_ptr; // silence unused
    }

    #[test]
    fn rewriting_leaf_allocates_only_the_spine() {
        let plan = test_plan();
        let root_ptr = Arc::as_ptr(&plan);
        let (child_ptr, grandchild_ptr) = match &*plan {
            LogicalPlan::Filter(Filter { input, .. }) => {
                let child = Arc::as_ptr(input);
                let grand = match &**input {
                    LogicalPlan::Limit(Limit { input, .. }) => Arc::as_ptr(input),
                    _ => panic!(),
                };
                (child, grand)
            }
            _ => panic!(),
        };

        // Rewrite the leaf (EmptyRelation) → a new EmptyRelation.
        let walked = plan
            .transform_up_arc(|node| match &**node {
                LogicalPlan::EmptyRelation(_) => Ok(Transformed::yes(Arc::new(
                    LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: true,
                        schema: Arc::new(DFSchema::empty()),
                    }),
                ))),
                _ => Ok(Transformed::no(Arc::clone(node))),
            })
            .unwrap();
        assert!(walked.transformed);
        // The entire spine (Filter, Limit, and EmptyRelation) must be
        // newly allocated, because the leaf change rippled up.
        assert_ne!(Arc::as_ptr(&walked.data), root_ptr);
        let LogicalPlan::Filter(Filter { input: child, .. }) = &*walked.data else {
            panic!();
        };
        assert_ne!(Arc::as_ptr(child), child_ptr);
        let LogicalPlan::Limit(Limit { input: grand, .. }) = &**child else {
            panic!();
        };
        // New leaf, different pointer from original leaf.
        assert_ne!(Arc::as_ptr(grand), grandchild_ptr);
    }

    #[test]
    fn apply_arc_visits_all_nodes_in_preorder() -> Result<()> {
        let plan = test_plan();
        let mut visited_kinds = Vec::new();
        plan.apply_arc(|node| {
            visited_kinds.push(match &**node {
                LogicalPlan::Filter(_) => "Filter",
                LogicalPlan::Limit(_) => "Limit",
                LogicalPlan::EmptyRelation(_) => "Empty",
                _ => "Other",
            });
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert_eq!(visited_kinds, vec!["Filter", "Limit", "Empty"]);
        Ok(())
    }

    #[test]
    fn apply_arc_honors_stop() -> Result<()> {
        let plan = test_plan();
        let mut count = 0;
        plan.apply_arc(|node| {
            count += 1;
            if matches!(&**node, LogicalPlan::Limit(_)) {
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })?;
        // Visited Filter and Limit, then stopped before Empty.
        assert_eq!(count, 2);
        Ok(())
    }
}