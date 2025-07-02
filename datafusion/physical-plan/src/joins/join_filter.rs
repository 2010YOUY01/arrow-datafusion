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

use crate::joins::utils::ColumnIndex;
use arrow::datatypes::SchemaRef;
use datafusion_common::JoinSide;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use std::sync::Arc;

/// Filter applied before join output. Fields are crate-public to allow
/// downstream implementations to experiment with custom joins.
///
/// Illustration:
/// ```text
/// left_plan: [v1, v2]
/// right_plan: [v3]
/// join_filter: v1 + v3 = 10
///
/// The join executor requires a minimal intermediate batch to evaluate the join
/// predicate (batch{[v1, v3]}), and it uses this struct to represent it.
/// The struct fields are:
/// - `expression`: v1 + v3 = 10
/// - `column_indices` is the index into the full join schema in join executor:
///   [ColumnIndex { index: 0, side: Left }, ColumnIndex { index: 0, side: Right }]
/// - `schema` is the schema of the intermediate batch (that is constructed
///   with projection to build a minimal batch to evaluate the join predicate:
///   [v1, v3]
/// ```
#[derive(Debug, Clone)]
pub struct JoinFilter {
    /// Filter expression
    pub(crate) expression: Arc<dyn PhysicalExpr>,
    /// Column indices required to construct intermediate batch for filtering
    pub(crate) column_indices: Vec<ColumnIndex>,
    /// Physical schema of intermediate batch
    pub(crate) schema: SchemaRef,
}

impl JoinFilter {
    /// Creates new JoinFilter
    pub fn new(
        expression: Arc<dyn PhysicalExpr>,
        column_indices: Vec<ColumnIndex>,
        schema: SchemaRef,
    ) -> JoinFilter {
        JoinFilter {
            expression,
            column_indices,
            schema,
        }
    }

    /// Helper for building ColumnIndex vector from left and right indices
    pub fn build_column_indices(
        left_indices: Vec<usize>,
        right_indices: Vec<usize>,
    ) -> Vec<ColumnIndex> {
        left_indices
            .into_iter()
            .map(|i| ColumnIndex {
                index: i,
                side: JoinSide::Left,
            })
            .chain(right_indices.into_iter().map(|i| ColumnIndex {
                index: i,
                side: JoinSide::Right,
            }))
            .collect()
    }

    /// Filter expression
    pub fn expression(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expression
    }

    /// Column indices for intermediate batch creation
    pub fn column_indices(&self) -> &[ColumnIndex] {
        &self.column_indices
    }

    /// Intermediate batch schema
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Rewrites the join filter if the inputs to the join are rewritten
    pub fn swap(&self) -> JoinFilter {
        let column_indices = self
            .column_indices()
            .iter()
            .map(|idx| ColumnIndex {
                index: idx.index,
                side: idx.side.negate(),
            })
            .collect();

        JoinFilter::new(
            Arc::clone(self.expression()),
            column_indices,
            Arc::clone(self.schema()),
        )
    }
}
