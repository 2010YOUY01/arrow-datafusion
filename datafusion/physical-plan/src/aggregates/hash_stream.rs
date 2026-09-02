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

//! 2-stage hash aggregation stream implementation.
//!
//! See comments in [`PartialHashAggregateStream`] and [`FinalHashAggregateStream`]
//! for details.
//!
//! Note these streams are an incremental migration of the existing
//! [`crate::aggregates::grouped_hash_stream::GroupedHashAggregateStream`].
//!
//! See issue for details: <https://github.com/apache/datafusion/issues/22710>

use std::mem::size_of;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result, internal_datafusion_err, internal_err};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::{TaskContext, TryEmitter, async_try_stream};
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use futures::stream::{Stream, StreamExt};

use super::AggregateExec;
use super::aggregate_hash_table::{
    AggregateHashTable, FinalMarker, OrderedAggregateTableMetrics, PartialMarker,
    PartialSkipMarker,
};
use super::ordered_final_stream::OrderedFinalAggregateStream;
use super::skip_partial::SkipAggregationProbe;
use crate::metrics::{
    BaselineMetrics, MetricBuilder, MetricCategory, RecordOutput, SpillMetrics,
};
use crate::sorts::IncrementalSortIterator;
use crate::sorts::streaming_merge::{SortedSpillFile, StreamingMergeBuilder};
use crate::spill::spill_manager::SpillManager;
use crate::stream::{EmptyRecordBatchStream, ObservedStream, RecordBatchStreamAdapter};
use crate::{InputOrderMode, RecordBatchStream, SendableRecordBatchStream, metrics};

/// Hash aggregation is implemented in two stages: partial and final. This
/// stream implements the partial stage.
///
/// # Example
///
/// SELECT k, AVG(v) FROM t GROUP BY k;
///
/// ## Plan
/// AggregateExec(stage=final)
/// -- RepartitionExec(hash(k))
/// ---- AggregateExec(stage=partial)
///
/// ## Partial Stage Behavior
/// Input: raw rows
/// Output: partial states for all groups (for example, `AVG(x)` emits `SUM(x)`
/// and `COUNT(x)`)
///
/// ## Final Stage Behavior
/// Input: partial states
/// Output: results for all groups (for example, `AVG(x)` calculated from the
/// state)
///
/// # Optimization: DISTINCT LIMIT Soft Limit
///
/// This optimization applies to both [`PartialHashAggregateStream`] and
/// [`FinalHashAggregateStream`].
///
/// Unordered distinct queries such as:
///
/// ```sql
/// SELECT DISTINCT x FROM t LIMIT 10;
/// ```
///
/// are optimized into a two-stage aggregate like:
///
/// ```txt
/// LimitExec, limit=10
/// --AggregateExec(Final), group_by=[x], aggr=[], soft_limit=10
/// ---- RepartitionExec, partitioning=hash(x)
/// ------ AggregateExec(Partial), group_by=[x], aggr=[], soft_limit=10
/// -------- Scan(t)
/// ```
///
/// After each input batch, the stream checks whether the soft limit has been
/// reached. If so, it emits the accumulated groups and stops reading input.
///
/// This operator does not guarantee an exact limit because a single batch can
/// cross the threshold. The downstream limit operator enforces the exact result
/// size.
///
/// # Optimization: Partial Aggregation Skip
///
/// Partial aggregation can be counterproductive for high-cardinality inputs,
/// where most rows create distinct groups. The stream probes the ratio of
/// accumulated groups to input rows while it is still aggregating. If the ratio
/// crosses the configured threshold and all aggregate accumulators can convert
/// raw inputs directly to partial state, the stream emits any already
/// accumulated groups, then switches to a skip state. In that state, each
/// remaining input batch is converted directly to partial aggregate state rows
/// without inserting the rows into the grouped hash table.
///
/// # Feature: Memory-limited Execution
///
/// ## Partial Aggregation
///
/// Partial aggregation can emit incomplete results because the final stage merges
/// all intermediate states for the same group. If the memory reservation exceeds
/// its limit after aggregating an input batch, this stream emits all accumulated
/// states and continues aggregating the remaining input with an empty table.
///
/// ## Final Aggregation
///
/// During final aggregation, group keys and states accumulate. If memory usage
/// exceeds the budget, spilling is triggered as follows:
/// 1. After aggregating a new input batch, if the memory reservation exceeds its
///    limit, spill all accumulated groups and states.
///    - Sort all groups by the group keys before spilling.
/// 2. Repeat until the input is exhausted.
/// 3. Perform a sort-preserving merge of all spill files and feed the merged output
///    into an ordered streaming aggregation, which ensures bounded memory usage and
///    evaluates the final result.
///    - [`OrderedFinalAggregateStream`] is reused for the streaming aggregation.
pub(crate) struct PartialHashAggregateStream {
    /// Output schema: group columns followed by partial aggregate state columns.
    schema: SchemaRef,

    /// Input batches containing raw rows, not partial aggregate state.
    input: SendableRecordBatchStream,

    /// Target output batch size from configuration.
    batch_size: usize,

    /// Memory reservation for group keys and accumulators.
    reservation: MemoryReservation,

    /// Execution metrics shared with the aggregate plan node.
    baseline_metrics: BaselineMetrics,

    /// Tracks partial aggregation row reduction, matching `GroupedHashAggregateStream`.
    reduction_factor: metrics::RatioMetrics,

    /// Tracks whether partial aggregation should switch to direct state conversion.
    skip_aggregation_probe: Option<SkipAggregationProbe>,

    /// Optional soft limit on the number of groups to accumulate before output.
    ///
    /// Invariant: when this is `Some(..)`, the accumulators inside `hash_table` must
    /// be empty. See struct comments for details.
    group_values_soft_limit: Option<usize>,

    /// Accumulates groups and partial states, and owns the lower-level state for
    /// emitting output batches.
    ///
    /// Only held here between construction and [`Self::into_stream`], which moves
    /// it into the generator. See [`Self::create_stream`].
    hash_table: Option<AggregateHashTable<PartialMarker>>,
}

/// States of partial hash aggregation. Each variant holds what its state works
/// on, and nothing else, so a state can be understood on its own. Each state is
/// handled by one method of [`PartialHashAggregateStream`], which returns the
/// next state.
///
/// See the state-transition graph in [`PartialHashAggregateStream::create_stream`].
enum PartialHashAggregateState {
    /// Aggregating input batches into the hash table.
    ReadingInput {
        hash_table: AggregateHashTable<PartialMarker>,
    },
    /// Emitting the states taken out of the table when it ran out of memory,
    /// before reading input continues with the emptied table.
    EmittingOnMemoryPressure {
        hash_table: AggregateHashTable<PartialMarker>,
        states: RecordBatch,
    },
    /// Emitting every accumulated group. If `skip_table` is `Some`, the probe
    /// decided to skip partial aggregation, and the remaining input is converted
    /// with that table afterwards; otherwise the stream is done.
    ProducingOutput {
        hash_table: AggregateHashTable<PartialMarker>,
        skip_table: Option<AggregateHashTable<PartialSkipMarker>>,
    },
    /// Converting the remaining input directly to partial states.
    SkippingAggregation {
        skip_table: AggregateHashTable<PartialSkipMarker>,
    },
    Done,
}

/// Spill configuration and accumulated runs for final hash aggregation.
///
/// Each spill event drains all currently buffered groups, sorts their intermediate
/// states by the full group key, and writes them to one spill file. All files are
/// merged and replayed after the original input ends.
struct FinalSpillContext {
    /// Aggregate configuration used to construct the final replay stream.
    final_agg: AggregateExec,
    /// Task context.
    context: Arc<TaskContext>,
    /// Original partition index.
    partition: usize,
    /// Target batch size from configuration.
    batch_size: usize,
    /// Full group-key ordering kept by every spill file and the merged input.
    spill_expr: LexOrdering,
    /// Spill I/O and metrics manager.
    spill_manager: SpillManager,
    /// Spill runs waiting to be merged, they're all sorted by full group-by keys.
    spills: Vec<SortedSpillFile>,
}

/// Hash aggregation is implemented in two stages: partial and final. This
/// stream implements the final stage.
///
/// See [`PartialHashAggregateStream`] for details.
pub(crate) struct FinalHashAggregateStream {
    /// Output schema: group columns followed by final aggregate value columns.
    schema: SchemaRef,

    /// Input batches containing partial aggregate state rows.
    input: SendableRecordBatchStream,

    /// Execution metrics shared with the aggregate plan node.
    baseline_metrics: BaselineMetrics,

    /// Memory reservation for group keys, accumulators, and spill sorting.
    reservation: MemoryReservation,

    /// See comments for the same variable in [`PartialHashAggregateStream`].
    group_values_soft_limit: Option<usize>,

    /// Tracks the high-level stream lifecycle. The hash table owns the lower-level
    /// state for emitting output batches.
    state: Option<FinalHashAggregateState>,
}

/// States for final hash aggregation processing.
// The typestate pattern is used in case the inner logic becomes more complex in
// the future.
enum FinalHashAggregateState {
    ReadingInput {
        hash_table: AggregateHashTable<FinalMarker>,
        /// `None` if spilling is not supported by the configured `DiskManager`.
        spill_context: Option<Box<FinalSpillContext>>,
    },
    Spilling {
        hash_table: AggregateHashTable<FinalMarker>,
        spill_context: Box<FinalSpillContext>,
    },
    ProducingOutput {
        hash_table: AggregateHashTable<FinalMarker>,
    },
    PreparingMergeInput {
        hash_table: AggregateHashTable<FinalMarker>,
        spill_context: Box<FinalSpillContext>,
    },
    MergingSpills {
        stream: SendableRecordBatchStream,
    },
    Done,
    /// Sentinel state to use when returning error from any other states, because:
    /// - It explicitly releases state-owned resources immediately
    /// - More defensive against accidentally resuming execution after error
    Error,
}

type FinalHashAggregatePoll = Poll<Option<Result<RecordBatch>>>;
type FinalHashAggregateStateTransition = ControlFlow<
    (FinalHashAggregatePoll, FinalHashAggregateState),
    FinalHashAggregateState,
>;

impl FinalSpillContext {
    fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
        batch_size: usize,
        spill_schema: &SchemaRef,
        spill_metrics: SpillMetrics,
    ) -> Result<Self> {
        let group_schema = agg.group_by.group_schema(&agg.input().schema())?;
        let output_ordering = agg.cache.output_ordering();
        let spill_sort_exprs =
            group_schema
                .fields()
                .iter()
                .enumerate()
                .map(|(idx, field)| {
                    let output_expr = Column::new(field.name(), idx);
                    let sort_options = output_ordering
                        .and_then(|ordering| ordering.get_sort_options(&output_expr))
                        .unwrap_or_default();
                    PhysicalSortExpr::new(Arc::new(output_expr), sort_options)
                });
        let Some(spill_expr) = LexOrdering::new(spill_sort_exprs) else {
            return internal_err!("Final hash aggregate spill expression is empty");
        };

        let spill_manager = SpillManager::new(
            context.runtime_env(),
            spill_metrics,
            Arc::clone(spill_schema),
        )
        .with_compression_type(context.session_config().spill_compression());

        let mut final_agg = agg.clone();
        final_agg.input_order_mode = InputOrderMode::Sorted;

        Ok(Self {
            final_agg,
            context: Arc::clone(context),
            partition,
            batch_size,
            spill_expr,
            spill_manager,
            spills: vec![],
        })
    }

    fn has_spills(&self) -> bool {
        !self.spills.is_empty()
    }

    /// Sorts and spills the aggregated groups. Memory reservation should be updated
    /// by the caller.
    ///
    /// Individual spill files are ordered by the `group by` keys.
    ///
    /// See [`FinalHashAggregateStream`] for spilling details.
    fn spill_table(
        &mut self,
        hash_table: &mut AggregateHashTable<FinalMarker>,
    ) -> Result<()> {
        let Some(batch) = hash_table.take_state_batch()? else {
            return Ok(());
        };

        let sorted_iter =
            IncrementalSortIterator::new(batch, self.spill_expr.clone(), self.batch_size);
        let spill_file = self
            .spill_manager
            .spill_record_batch_iter_and_return_max_batch_memory(
                sorted_iter,
                "FinalHashAggregateSpill",
            )?;

        let Some((file, max_record_batch_memory)) = spill_file else {
            return internal_err!("Final hash aggregation produced an empty spill");
        };

        self.spills.push(SortedSpillFile {
            file,
            max_record_batch_memory,
        });

        Ok(())
    }

    /// Merges every sorted run, and do the aggregate evaluation with
    /// [`OrderedFinalAggregateStream`]
    fn into_replay_stream(
        self,
        baseline_metrics: &BaselineMetrics,
        metrics: OrderedAggregateTableMetrics,
        reservation: MemoryReservation,
    ) -> Result<SendableRecordBatchStream> {
        let Self {
            final_agg,
            context,
            partition,
            batch_size,
            spill_expr,
            spill_manager,
            spills,
        } = self;

        let spill_schema = Arc::clone(spill_manager.schema());
        // The merge and replay table are two components of the same aggregate
        // operator. Keep them under one consumer registration so a fair memory
        // pool does not divide this operator's quota between its own phases.
        let merge_reservation = reservation.new_empty();
        let merged = StreamingMergeBuilder::new()
            .with_schema(spill_schema)
            .with_spill_manager(spill_manager)
            .with_sorted_spill_files(spills)
            .with_expressions(&spill_expr)
            .with_metrics(baseline_metrics.intermediate())
            .with_batch_size(batch_size)
            .with_reservation(merge_reservation)
            .build()?;
        let replay = OrderedFinalAggregateStream::new_with_input_and_metrics(
            &final_agg,
            &context,
            partition,
            merged,
            &InputOrderMode::Sorted,
            baseline_metrics.clone(),
            metrics,
            None,
            reservation,
        )?;
        Ok(Box::pin(replay))
    }
}

impl PartialHashAggregateStream {
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        debug_assert_eq!(agg.mode, super::AggregateMode::Partial);
        debug_assert_eq!(agg.input_order_mode, InputOrderMode::Linear);

        let schema = Arc::clone(&agg.schema);
        let input = agg.input.execute(partition, Arc::clone(context))?;
        let batch_size = context.session_config().batch_size();
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);

        // Preserve the existing aggregate metric surface for this plan node.
        let _spill_metrics = SpillMetrics::new(&agg.metrics, partition);
        let reduction_factor = MetricBuilder::new(&agg.metrics)
            .with_type(metrics::MetricType::Summary)
            .ratio_metrics("reduction_factor", partition);

        let hash_table = AggregateHashTable::<PartialMarker>::new(
            agg,
            partition,
            Arc::clone(&schema),
            batch_size,
        )?;
        let skip_aggregation_probe = if agg.group_by.is_single() {
            let options = &context.session_config().options().execution;
            let probe_ratio_threshold =
                options.skip_partial_aggregation_probe_ratio_threshold;
            // A threshold >= 1.0 means the ratio (num_groups / input_rows) can
            // never exceed it, so the feature is effectively disabled.
            if probe_ratio_threshold >= 1.0 {
                None
            } else {
                let skipped_aggregation_rows = MetricBuilder::new(&agg.metrics)
                    .with_category(MetricCategory::Rows)
                    .counter("skipped_aggregation_rows", partition);
                Some(SkipAggregationProbe::new(
                    options.skip_partial_aggregation_probe_rows_threshold,
                    probe_ratio_threshold,
                    skipped_aggregation_rows,
                ))
            }
        } else {
            None
        };

        let reservation =
            MemoryConsumer::new(format!("PartialHashAggregateStream[{partition}]"))
                .with_can_spill(true)
                .register(context.memory_pool());

        Ok(Self {
            schema,
            input,
            batch_size,
            baseline_metrics,
            reservation,
            reduction_factor,
            skip_aggregation_probe,
            group_values_soft_limit: agg.limit_options().map(|config| config.limit()),
            hash_table: Some(hash_table),
        })
    }

    pub(crate) fn into_stream(self) -> SendableRecordBatchStream {
        let schema = Arc::clone(&self.schema);
        let baseline_metrics = self.baseline_metrics.clone();
        let stream =
            Box::pin(RecordBatchStreamAdapter::new(schema, self.create_stream()));

        // Records output rows and the end time of `baseline_metrics`.
        Box::pin(ObservedStream::new(stream, baseline_metrics, None))
    }

    /// Entry point for the partial hash aggregate state machine.
    ///
    /// See comments in [`PartialHashAggregateStream`] for high-level ideas.
    ///
    /// State transitions are implemented using the generator pattern; see the
    /// comments in [`async_try_stream`]. Each state below is one variant of
    /// [`PartialHashAggregateState`], holding what the state works on, and one
    /// method that does the work and returns the next state. This function only
    /// dispatches.
    ///
    /// Conceptual state-transition graph:
    ///
    /// ```text
    /// (start)
    ///   -> ReadingInput
    ///      The stream starts by polling input and aggregating batches into the
    ///      in-memory hash table.
    ///
    /// ReadingInput
    ///   Aggregate one batch into the hash table, then check the exits below in
    ///   this order.
    ///   -> ProducingOutput
    ///      Input was exhausted, or the soft group limit was reached. Close the
    ///      input, then output the accumulated groups.
    ///   -> ProducingOutput, then SkippingAggregation
    ///      The probe decided to skip partial aggregation. Output the accumulated
    ///      groups first, then convert the remaining input directly to partial
    ///      states without aggregation.
    ///   -> EmittingOnMemoryPressure
    ///      The table cannot reserve enough memory. Take all accumulated partial
    ///      states out of the table, and emit them incrementally.
    ///   -> ReadingInput
    ///      Otherwise, continue with the next input batch.
    ///
    /// EmittingOnMemoryPressure
    ///   -> EmittingOnMemoryPressure
    ///      One batch-sized slice was yielded; repeat until all taken partial
    ///      states are emitted.
    ///   -> ReadingInput
    ///      All taken states were emitted; continue reading with the empty table.
    ///
    /// ProducingOutput
    ///   -> ProducingOutput
    ///      One output batch was yielded; repeat until the table is empty.
    ///   -> Done
    ///      All accumulated groups were emitted.
    ///   -> SkippingAggregation
    ///      All accumulated groups were emitted, and the probe had decided to
    ///      skip partial aggregation.
    ///
    /// SkippingAggregation
    ///   -> SkippingAggregation
    ///      One input batch was converted to partial states and yielded; repeat
    ///      with the next input batch.
    ///   -> Done
    ///      Input was exhausted.
    ///
    /// Done
    ///   -> (end)
    /// ```
    ///
    /// An error ends the stream from any state. The generator owns every resource
    /// (the input, the hash tables, and the memory reservation), so returning the
    /// error drops and releases all of them.
    fn create_stream(mut self) -> impl Stream<Item = Result<RecordBatch>> {
        async_try_stream(|mut emitter| async move {
            let hash_table = self
                .hash_table
                .take()
                .expect("PartialHashAggregateStream hash table should not be None");
            let mut state = PartialHashAggregateState::ReadingInput { hash_table };

            loop {
                state = match state {
                    PartialHashAggregateState::ReadingInput { hash_table } => {
                        self.read_input(hash_table).await?
                    }
                    PartialHashAggregateState::EmittingOnMemoryPressure {
                        hash_table,
                        states,
                    } => {
                        self.emit_on_memory_pressure(hash_table, states, &mut emitter)
                            .await
                    }
                    PartialHashAggregateState::ProducingOutput {
                        hash_table,
                        skip_table,
                    } => {
                        self.produce_output(hash_table, skip_table, &mut emitter)
                            .await?
                    }
                    PartialHashAggregateState::SkippingAggregation { skip_table } => {
                        self.skip_aggregation(skip_table, &mut emitter).await?
                    }
                    PartialHashAggregateState::Done => return Ok(()),
                };
            }
        })
    }

    /// Aggregates input batches into the hash table until the input is exhausted,
    /// the soft group limit is reached, the probe decides to skip partial
    /// aggregation, or the table runs out of memory. This state never yields
    /// output itself: emitting is left to the state it transitions to.
    ///
    /// See comments at [`Self::create_stream`] for details.
    async fn read_input(
        &mut self,
        mut hash_table: AggregateHashTable<PartialMarker>,
    ) -> Result<PartialHashAggregateState> {
        debug_assert!(hash_table.is_building());
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();

        while let Some(batch) = self.input.next().await.transpose()? {
            let _timer = elapsed_compute.timer();
            let input_rows = batch.num_rows();
            self.reduction_factor.add_total(input_rows);
            hash_table.aggregate_batch(&batch)?;

            // The exits below are checked in this order, and the first one that
            // fires wins.
            if self.hit_soft_group_limit(&hash_table) {
                // The remaining input is ignored.
                self.close_input();
                return Ok(PartialHashAggregateState::ProducingOutput {
                    hash_table,
                    skip_table: None,
                });
            }

            if self.probe_skip_aggregation(input_rows, hash_table.building_group_count())
            {
                // The skip table copies the accumulator definitions from the
                // table's building state, so it must be built before the table
                // starts outputting in `ProducingOutput`.
                let skip_table = hash_table.partial_skip_table()?;
                return Ok(PartialHashAggregateState::ProducingOutput {
                    hash_table,
                    skip_table: Some(skip_table),
                });
            }

            // Check memory reservation. See function comments for details.
            if let Some(states) = self.resize_or_take_states(&mut hash_table)? {
                return Ok(PartialHashAggregateState::EmittingOnMemoryPressure {
                    hash_table,
                    states,
                });
            }
        }

        // No more input is read: release it (and whatever upstream holds for it)
        // now, instead of when the generator finishes.
        self.close_input();
        Ok(PartialHashAggregateState::ProducingOutput {
            hash_table,
            skip_table: None,
        })
    }

    /// Updates the memory reservation to the table's current size, and:
    /// - If the reservation succeeds, returns `Ok(None)`: keep aggregating.
    /// - If it fails with out-of-memory:
    ///   - and groups are accumulated, takes every partial state out of the
    ///     table, shrinks the reservation to the emptied table, and returns
    ///     `Ok(Some(states))` for [`Self::emit_on_memory_pressure`] to emit. The
    ///     final stage merges repeated states of the same group, so emitting
    ///     incomplete results is correct.
    ///   - and no group is accumulated, early emission cannot release any
    ///     memory, so the original out-of-memory error is returned.
    ///
    /// # Implementation Note
    /// All accumulated states are materialized at once here, and
    /// [`Self::emit_on_memory_pressure`] slices them into `batch_size` output
    /// batches. Emit them incrementally after blocked state management is ready.
    ///
    /// Issue: <https://github.com/apache/datafusion/issues/7065>
    fn resize_or_take_states(
        &mut self,
        hash_table: &mut AggregateHashTable<PartialMarker>,
    ) -> Result<Option<RecordBatch>> {
        let oom = match self.reservation.try_resize(hash_table.memory_size()) {
            Ok(()) => return Ok(None),
            Err(e @ DataFusionError::ResourcesExhausted(_)) => e,
            Err(e) => return Err(e),
        };

        let Some(states) = hash_table.take_state_batch()? else {
            // Nothing is accumulated, so early emission cannot release memory.
            return Err(oom);
        };
        // Taking the states cleared the table; shrink the reservation to match.
        self.reservation.try_resize(hash_table.memory_size())?;
        Ok(Some(states))
    }

    /// Emits partial states taken out of the table under memory pressure, in
    /// `batch_size` slices, then resumes reading input with the emptied table.
    ///
    /// See comments at [`Self::create_stream`] for details.
    async fn emit_on_memory_pressure(
        &mut self,
        hash_table: AggregateHashTable<PartialMarker>,
        states: RecordBatch,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> PartialHashAggregateState {
        debug_assert!(states.num_rows() > 0);

        for offset in (0..states.num_rows()).step_by(self.batch_size) {
            let length = self.batch_size.min(states.num_rows() - offset);
            let output = states.slice(offset, length);
            self.reduction_factor.add_part(output.num_rows());
            emitter.emit(output).await;
        }

        PartialHashAggregateState::ReadingInput { hash_table }
    }

    /// Emits every accumulated group as partial-state batches, then releases the
    /// table and its memory reservation. Continues with skipping aggregation if
    /// `skip_table` is `Some`, and is done otherwise.
    ///
    /// See comments at [`Self::create_stream`] for details.
    async fn produce_output(
        &mut self,
        mut hash_table: AggregateHashTable<PartialMarker>,
        skip_table: Option<AggregateHashTable<PartialSkipMarker>>,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<PartialHashAggregateState> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let mut timer = elapsed_compute.timer();

        hash_table.start_output()?;
        while let Some(batch) = hash_table.next_output_batch()? {
            debug_assert!(batch.num_rows() > 0);
            // The output is already materialized, so a failed resize cannot be
            // acted on: keep the reservation as is and finish the output.
            let _ = self.reservation.try_resize(hash_table.memory_size());
            self.reduction_factor.add_part(batch.num_rows());

            timer.done();
            emitter.emit(batch).await;
            timer = elapsed_compute.timer();
        }
        timer.done();

        drop(hash_table);
        self.reservation.free();

        Ok(match skip_table {
            Some(skip_table) => {
                PartialHashAggregateState::SkippingAggregation { skip_table }
            }
            None => PartialHashAggregateState::Done,
        })
    }

    /// Converts each remaining raw input batch directly to partial states, without
    /// inserting its rows into a hash table, until the input is exhausted.
    ///
    /// See comments at [`Self::create_stream`] for details.
    async fn skip_aggregation(
        &mut self,
        mut skip_table: AggregateHashTable<PartialSkipMarker>,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<PartialHashAggregateState> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();

        while let Some(batch) = self.input.next().await.transpose()? {
            if let Some(probe) = self.skip_aggregation_probe.as_mut() {
                probe.record_skipped(&batch);
            }

            let timer = elapsed_compute.timer();
            let states = skip_table.convert_batch_to_state(&batch)?;
            timer.done();

            emitter.emit(states).await;
        }

        Ok(PartialHashAggregateState::Done)
    }

    /// Drops the input stream, releasing the resources it and its upstream
    /// operators hold, before the generator finishes.
    fn close_input(&mut self) {
        let input_schema = self.input.schema();
        self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
    }

    /// See comments in [`Self::group_values_soft_limit`] for details.
    fn hit_soft_group_limit(
        &self,
        hash_table: &AggregateHashTable<PartialMarker>,
    ) -> bool {
        self.group_values_soft_limit
            .is_some_and(|limit| limit <= hash_table.building_group_count())
    }

    /// Feeds one input batch's statistics to the skip aggregation probe, and
    /// returns whether it decided to skip partial aggregation. See struct comments
    /// for details.
    fn probe_skip_aggregation(&mut self, input_rows: usize, num_groups: usize) -> bool {
        let Some(probe) = self.skip_aggregation_probe.as_mut() else {
            return false;
        };
        probe.update_state(input_rows, num_groups);
        probe.should_skip()
    }
}

impl FinalHashAggregateStream {
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        debug_assert!(matches!(
            agg.mode,
            super::AggregateMode::Final | super::AggregateMode::FinalPartitioned
        ));
        debug_assert_eq!(agg.input_order_mode, InputOrderMode::Linear);

        let schema = Arc::clone(&agg.schema);
        let input = agg.input.execute(partition, Arc::clone(context))?;
        let input_schema = input.schema();
        let batch_size = context.session_config().batch_size();
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);
        let spill_metrics = SpillMetrics::new(&agg.metrics, partition);

        let hash_table = AggregateHashTable::<FinalMarker>::new(
            agg,
            partition,
            Arc::clone(&schema),
            batch_size,
        )?;

        let can_spill = context.runtime_env().disk_manager.tmp_files_enabled();
        let spill_context = if can_spill {
            Some(Box::new(FinalSpillContext::new(
                agg,
                context,
                partition,
                batch_size,
                &input_schema,
                spill_metrics,
            )?))
        } else {
            None
        };

        let reservation =
            MemoryConsumer::new(format!("FinalHashAggregateStream[{partition}]"))
                .with_can_spill(can_spill)
                .register(context.memory_pool());

        Ok(Self {
            schema,
            input,
            baseline_metrics,
            reservation,
            group_values_soft_limit: agg.limit_options().map(|config| config.limit()),
            state: Some(FinalHashAggregateState::ReadingInput {
                hash_table,
                spill_context,
            }),
        })
    }

    fn close_input(&mut self) {
        let input_schema = self.input.schema();
        self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
    }

    fn break_with_err(error: DataFusionError) -> FinalHashAggregateStateTransition {
        ControlFlow::Break((
            Poll::Ready(Some(Err(error))),
            FinalHashAggregateState::Error,
        ))
    }

    fn break_with_internal_err(
        message: impl std::fmt::Display,
    ) -> FinalHashAggregateStateTransition {
        Self::break_with_err(internal_datafusion_err!("{message}"))
    }

    /// See comments in [`Self::group_values_soft_limit`] for details.
    fn hit_soft_group_limit(&self, hash_table: &AggregateHashTable<FinalMarker>) -> bool {
        self.group_values_soft_limit
            .is_some_and(|limit| limit <= hash_table.building_group_count())
    }

    fn start_output(
        &mut self,
        hash_table: &mut AggregateHashTable<FinalMarker>,
    ) -> Result<()> {
        self.close_input();
        hash_table.start_output()
    }

    /// Reserve memory for the current aggregate table.
    fn reservation_size_for_table(
        hash_table: &AggregateHashTable<FinalMarker>,
        spill_context: Option<&FinalSpillContext>,
    ) -> usize {
        let table_size = hash_table.memory_size();
        if spill_context.is_some() {
            // Count extra space needed for in-memory sorting and spilling. Only
            // count memory for indices, the payload will be materialize incrementally
            // in smaller chunks.
            table_size.saturating_add(
                hash_table
                    .building_group_count()
                    .saturating_mul(size_of::<u32>()),
            )
        } else {
            table_size
        }
    }

    /// Handle ReadingInput state - aggregate partial state batches into the hash table.
    ///
    /// See comments at `poll_next()` for details.
    ///
    /// Returns the next operator state with control flow decision.
    fn handle_reading_input(
        &mut self,
        cx: &mut Context<'_>,
        original_state: FinalHashAggregateState,
    ) -> FinalHashAggregateStateTransition {
        let FinalHashAggregateState::ReadingInput {
            mut hash_table,
            spill_context,
        } = original_state
        else {
            return Self::break_with_internal_err(
                "Final hash aggregate stream expected ReadingInput state",
            );
        };

        match self.input.poll_next_unpin(cx) {
            Poll::Pending => ControlFlow::Break((
                Poll::Pending,
                FinalHashAggregateState::ReadingInput {
                    hash_table,
                    spill_context,
                },
            )),
            Poll::Ready(Some(Ok(batch))) => {
                let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
                let timer = elapsed_compute.timer();
                let result = hash_table.aggregate_batch(&batch);
                timer.done();

                if let Err(e) = result {
                    return Self::break_with_err(e);
                }

                // Soft group limits are usually small and rarely coincide with
                // spilling. Once spilling has occurred, skip this optimization to
                // make the internal logic simpler.
                let spilled = spill_context
                    .as_ref()
                    .is_some_and(|context| context.has_spills());
                if self.hit_soft_group_limit(&hash_table) && !spilled {
                    let timer = elapsed_compute.timer();
                    let result = self.start_output(&mut hash_table);
                    timer.done();

                    return match result {
                        Ok(()) => ControlFlow::Continue(
                            FinalHashAggregateState::ProducingOutput { hash_table },
                        ),
                        Err(e) => Self::break_with_err(e),
                    };
                }

                // Check memory reservation, and potentially spill.
                let timer = elapsed_compute.timer();
                let resize_result =
                    self.reservation
                        .try_resize(Self::reservation_size_for_table(
                            &hash_table,
                            spill_context.as_deref(),
                        ));
                timer.done();
                match resize_result {
                    Ok(()) => {}
                    Err(e @ DataFusionError::ResourcesExhausted(_)) => {
                        // OOM and don't support spilling from configuration
                        let Some(spill_context) = spill_context else {
                            return Self::break_with_err(e.context(
                                "Final hash aggregate cannot spill because temporary files are not enabled in the DiskManager",
                            ));
                        };
                        // Sanity check: impossible to OOM when there is no group aggregated.
                        if hash_table.building_group_count() == 0 {
                            return Self::break_with_internal_err(
                                "Final hash aggregate ran out of memory with no aggregated groups",
                            );
                        }
                        // Go to the next state to perform spilling the aggregated
                        // groups so far.
                        return ControlFlow::Continue(
                            FinalHashAggregateState::Spilling {
                                hash_table,
                                spill_context,
                            },
                        );
                    }
                    Err(e) => return Self::break_with_err(e),
                }

                ControlFlow::Continue(FinalHashAggregateState::ReadingInput {
                    hash_table,
                    spill_context,
                })
            }
            Poll::Ready(Some(Err(e))) => Self::break_with_err(e),
            // Input done, move to next state:
            // - If spilled before, perform merging spill runs
            // - If not spilled, start producing outputs
            Poll::Ready(None) => {
                self.close_input();
                match spill_context {
                    Some(spill_context) if spill_context.has_spills() => {
                        ControlFlow::Continue(
                            FinalHashAggregateState::PreparingMergeInput {
                                hash_table,
                                spill_context,
                            },
                        )
                    }
                    _ => {
                        let elapsed_compute =
                            self.baseline_metrics.elapsed_compute().clone();
                        let timer = elapsed_compute.timer();
                        let result = hash_table.start_output();
                        timer.done();

                        match result {
                            Ok(()) => ControlFlow::Continue(
                                FinalHashAggregateState::ProducingOutput { hash_table },
                            ),
                            Err(e) => Self::break_with_err(e),
                        }
                    }
                }
            }
        }
    }

    /// Sorts and spills one complete in-memory state run, then resumes input.
    ///
    /// See comments at `poll_next()` for details.
    ///
    /// Returns the next operator state with control flow decision.
    fn handle_spilling(
        &mut self,
        original_state: FinalHashAggregateState,
    ) -> FinalHashAggregateStateTransition {
        let FinalHashAggregateState::Spilling {
            mut hash_table,
            mut spill_context,
        } = original_state
        else {
            return Self::break_with_internal_err(
                "Final hash aggregate stream expected Spilling state",
            );
        };

        // Sanity check: it is impossible to OOM when the table is empty.
        if hash_table.building_group_count() == 0 {
            return Self::break_with_internal_err(
                "Final hash aggregation entered Spilling with an empty table",
            );
        }

        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let timer = elapsed_compute.timer();
        let mut result = spill_context.spill_table(&mut hash_table);

        // Spilling shrinks the aggregate table and releases its accumulated
        // memory. Update the reservation accordingly.
        if let Err(e) = self.reservation.try_resize(hash_table.memory_size()) {
            result =
                Err(e.context("Decreasing allocation after spilling should succeed"));
        }

        timer.done();

        match result {
            // Finished spilling the aggregate table, continue aggregating from input.
            Ok(()) => ControlFlow::Continue(FinalHashAggregateState::ReadingInput {
                hash_table,
                spill_context: Some(spill_context),
            }),
            Err(e) => Self::break_with_err(e),
        }
    }

    /// 1. Spills the last in-memory run.
    /// 2. Constructs a globally ordered input stream by applying a sort-preserving
    ///    merge to all spills.
    /// 3. Constructs a replay stream: an ordered final aggregate stream over the
    ///    fully ordered input constructed from the spills.
    ///
    /// See comments at `poll_next()` for details.
    ///
    /// Returns the next operator state with control flow decision.
    fn handle_preparing_merge_input(
        &mut self,
        original_state: FinalHashAggregateState,
    ) -> FinalHashAggregateStateTransition {
        let FinalHashAggregateState::PreparingMergeInput {
            mut hash_table,
            mut spill_context,
        } = original_state
        else {
            return Self::break_with_internal_err(
                "Final hash aggregate stream expected PreparingMergeInput state",
            );
        };

        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let timer = elapsed_compute.timer();
        let replay = match spill_context.spill_table(&mut hash_table) {
            Ok(()) => {
                let metrics = OrderedAggregateTableMetrics::from_hash_table(&hash_table);
                drop(hash_table);
                match self.reservation.try_resize(0) {
                    Ok(()) => (*spill_context).into_replay_stream(
                        &self.baseline_metrics,
                        metrics,
                        self.reservation.new_empty(),
                    ),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        };
        timer.done();

        match replay {
            Ok(stream) => {
                ControlFlow::Continue(FinalHashAggregateState::MergingSpills { stream })
            }
            Err(e) => Self::break_with_err(e),
        }
    }

    /// Forwards output from the fully ordered stream that consumes the merged
    /// spill runs.
    ///
    /// See comments at `poll_next()` for details.
    ///
    /// Returns the next operator state with control flow decision.
    fn handle_merging_spills(
        &mut self,
        cx: &mut Context<'_>,
        original_state: FinalHashAggregateState,
    ) -> FinalHashAggregateStateTransition {
        let FinalHashAggregateState::MergingSpills { mut stream } = original_state else {
            return Self::break_with_internal_err(
                "Final hash aggregate stream expected MergingSpills state",
            );
        };

        match stream.poll_next_unpin(cx) {
            Poll::Pending => ControlFlow::Break((
                Poll::Pending,
                FinalHashAggregateState::MergingSpills { stream },
            )),
            Poll::Ready(Some(Ok(batch))) => ControlFlow::Break((
                Poll::Ready(Some(Ok(batch))),
                FinalHashAggregateState::MergingSpills { stream },
            )),
            Poll::Ready(Some(Err(e))) => Self::break_with_err(e),
            Poll::Ready(None) => ControlFlow::Continue(FinalHashAggregateState::Done),
        }
    }

    /// Handle ProducingOutput state - emit final aggregate value batches.
    ///
    /// See comments at `poll_next()` for details.
    ///
    /// Returns the next operator state with control flow decision.
    fn handle_producing_output(
        &mut self,
        original_state: FinalHashAggregateState,
    ) -> FinalHashAggregateStateTransition {
        let FinalHashAggregateState::ProducingOutput { mut hash_table } = original_state
        else {
            return Self::break_with_internal_err(
                "Final hash aggregate stream expected ProducingOutput state",
            );
        };

        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let timer = elapsed_compute.timer();
        let result = hash_table.next_output_batch();
        timer.done();

        match result {
            Ok(Some(batch)) => {
                let next_state = if hash_table.is_done() {
                    drop(hash_table);
                    if let Err(e) = self.reservation.try_resize(0) {
                        return Self::break_with_err(e);
                    }
                    FinalHashAggregateState::Done
                } else {
                    if let Err(e) = self.reservation.try_resize(hash_table.memory_size())
                    {
                        return Self::break_with_err(e);
                    }
                    FinalHashAggregateState::ProducingOutput { hash_table }
                };

                ControlFlow::Break((
                    Poll::Ready(Some(Ok(batch.record_output(&self.baseline_metrics)))),
                    next_state,
                ))
            }
            Err(e) => Self::break_with_err(e),
            Ok(None) => {
                drop(hash_table);
                let next_state = FinalHashAggregateState::Done;
                if let Err(e) = self.reservation.try_resize(0) {
                    return Self::break_with_err(e);
                }
                ControlFlow::Continue(next_state)
            }
        }
    }
}

impl Stream for FinalHashAggregateStream {
    type Item = Result<RecordBatch>;

    /// Entry point for the final hash aggregate state machine.
    ///
    /// See comments in [`FinalHashAggregateStream`] for high-level ideas.
    ///
    /// State transition graph:
    ///
    /// ```text
    /// (start)
    ///   -> ReadingInput
    ///      The stream starts by polling partial-state input and aggregating
    ///      those states into the final hash table.
    ///
    /// ReadingInput
    ///   -> ReadingInput
    ///      Aggregate one partial-state input batch. If it fits in memory,
    ///      continue with the next input batch.
    ///   -> Spilling
    ///      The table cannot reserve enough memory. Move all current states into
    ///      one fully group-key-sorted spill run.
    ///   -> ProducingOutput
    ///      Input was exhausted without spilling, or the soft group limit was
    ///      reached. Start outputting final aggregate values.
    ///   -> PreparingMergeInput
    ///      Input was exhausted after spilling. Spill the last in-memory run and
    ///      construct the ordered input used to merge all spill files.
    ///
    /// Spilling
    ///   -> ReadingInput
    ///      One sorted run was written; resume reading the original input.
    ///
    /// PreparingMergeInput
    ///   Spill the final in-memory run and build the input ordered replay stream.
    ///   -> MergingSpills
    ///      The final run was spilled and the ordered replay stream was built.
    ///
    /// MergingSpills
    ///   Aggregate the merged spill runs and emit final results.
    ///   -> MergingSpills
    ///      Forward one result batch from the fully ordered replay stream that
    ///      consumes the sort-preserving merge.
    ///   -> Done
    ///      The merged spill input was fully aggregated.
    ///
    /// ProducingOutput
    ///   -> ProducingOutput
    ///      One final output batch was yielded; repeat to continue producing
    ///      output incrementally.
    ///   -> Done
    ///      All final output was emitted.
    ///
    /// Any active state
    ///   -> Error
    ///      An error drops state-owned resources before it is returned.
    ///
    /// Error
    ///   -> (end)
    ///
    /// Done
    ///   -> (end)
    /// ```
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            let cur_state = self
                .state
                .take()
                .expect("FinalHashAggregateStream state should not be None");

            let next_state = match cur_state {
                state @ FinalHashAggregateState::ReadingInput { .. } => {
                    self.handle_reading_input(cx, state)
                }
                state @ FinalHashAggregateState::Spilling { .. } => {
                    self.handle_spilling(state)
                }
                state @ FinalHashAggregateState::PreparingMergeInput { .. } => {
                    self.handle_preparing_merge_input(state)
                }
                state @ FinalHashAggregateState::MergingSpills { .. } => {
                    self.handle_merging_spills(cx, state)
                }
                state @ FinalHashAggregateState::ProducingOutput { .. } => {
                    self.handle_producing_output(state)
                }
                state @ FinalHashAggregateState::Error => {
                    self.close_input();
                    self.reservation.free();
                    self.state = Some(state);
                    return Poll::Ready(None);
                }
                state @ FinalHashAggregateState::Done => {
                    let _ = self.reservation.try_resize(0);
                    self.state = Some(state);
                    return Poll::Ready(None);
                }
            };

            match next_state {
                ControlFlow::Continue(next_state) => {
                    self.state = Some(next_state);
                }
                ControlFlow::Break((Poll::Ready(Some(Err(e))), next_state)) => {
                    debug_assert!(matches!(next_state, FinalHashAggregateState::Error));

                    // The handler has already discarded its state-owned resources.
                    // Release the remaining stream-owned resources before returning.
                    self.close_input();
                    self.reservation.free();
                    self.state = Some(FinalHashAggregateState::Error);
                    return Poll::Ready(Some(Err(e)));
                }
                ControlFlow::Break((poll, next_state)) => {
                    self.state = Some(next_state);
                    return poll;
                }
            }
        }
    }
}

impl RecordBatchStream for FinalHashAggregateStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::aggregates::{AggregateMode, PhysicalGroupBy};
    use crate::common::collect;
    use crate::execution_plan::ExecutionPlan;
    use crate::test::TestMemoryExec;
    use crate::test::exec::MockExec;

    use arrow::array::{AsArray, Float64Array, Int32Array, Int64Array, UInt32Array};
    use arrow::datatypes::{DataType, Field, Float64Type, Schema};
    use datafusion_common::{Result, ScalarValue, assert_contains, exec_err};
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_aggregate::sum::sum_udaf;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::col;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_partial_hash_stream_double_emission_race_condition_bug() -> Result<()> {
        // Fix for https://github.com/apache/datafusion/issues/18701
        // This test specifically proves that we have fixed double emission race condition
        // where emit_early_if_necessary() and switch_to_skip_aggregation()
        // both emit in the same loop iteration, causing data loss

        let schema = Arc::new(Schema::new(vec![
            Field::new("group_col", DataType::Int32, false),
            Field::new("value_col", DataType::Int64, false),
        ]));

        // Create data that will trigger BOTH conditions in the same iteration:
        // 1. More groups than batch_size (triggers early emission when memory pressure hits)
        // 2. High cardinality ratio (triggers skip aggregation)
        let batch_size = 1024; // We'll set this in session config
        let num_groups = batch_size + 100; // Slightly more than batch_size (1124 groups)

        // Create exactly 1 row per group = 100% cardinality ratio
        let group_ids: Vec<i32> = (0..num_groups as i32).collect();
        let values: Vec<i64> = vec![1; num_groups];

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(group_ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )?;
        let input_partitions = vec![vec![batch]];

        // Create constrained memory to trigger early emission but not completely fail
        let runtime = RuntimeEnvBuilder::default()
            .with_memory_limit(1024, 1.0) // small enough to start but will trigger pressure
            .build_arc()?;

        let mut task_ctx = TaskContext::default().with_runtime(runtime);

        // Configure to trigger BOTH conditions:
        // 1. Low probe threshold (triggers skip probe after few rows)
        // 2. Low ratio threshold (triggers skip aggregation immediately)
        // 3. Set batch_size to 1024 so our 1124 groups will trigger early emission
        // This creates the race condition where both emit paths are triggered
        let mut session_config = task_ctx.session_config().clone();
        session_config = session_config.set(
            "datafusion.execution.batch_size",
            &ScalarValue::UInt64(Some(1024)),
        );
        session_config = session_config.set(
            "datafusion.execution.skip_partial_aggregation_probe_rows_threshold",
            &ScalarValue::UInt64(Some(50)),
        );
        session_config = session_config.set(
            "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
            &ScalarValue::Float64(Some(0.8)),
        );
        task_ctx = task_ctx.with_session_config(session_config);
        let task_ctx = Arc::new(task_ctx);

        // Create aggregate: COUNT(*) GROUP BY group_col
        let group_expr = vec![(col("group_col", &schema)?, "group_col".to_string())];
        let aggr_expr = vec![Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![col("value_col", &schema)?])
                .schema(Arc::clone(&schema))
                .alias("count_value")
                .build()?,
        )];

        let exec = TestMemoryExec::try_new(&input_partitions, Arc::clone(&schema), None)?;
        let exec = Arc::new(TestMemoryExec::update_cache(&Arc::new(exec)));

        // Use Partial mode where the race condition occurs
        let aggregate_exec = AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(group_expr),
            aggr_expr,
            vec![None],
            exec,
            Arc::clone(&schema),
        )?;

        // Execute and collect results
        let mut stream =
            PartialHashAggregateStream::new(&aggregate_exec, &Arc::clone(&task_ctx), 0)?
                .into_stream();
        let mut results = Vec::new();

        while let Some(result) = stream.next().await {
            let batch = result?;
            results.push(batch);
        }

        // Count total groups emitted
        let mut total_output_groups = 0;
        for batch in &results {
            total_output_groups += batch.num_rows();
        }

        assert_eq!(
            total_output_groups, num_groups,
            "Unexpected number of groups",
        );
        // Both the drained groups and the skipped rows are counted as output.
        let metrics = aggregate_exec.metrics().unwrap();
        assert_eq!(metrics.output_rows(), Some(num_groups));

        Ok(())
    }

    #[tokio::test]
    async fn test_partial_hash_stream_skip_aggregation_probe_not_locked_until_skip()
    -> Result<()> {
        // Test that the probe is not locked until we actually decide to skip.
        // This allows us to continue evaluating the skip condition across multiple batches.
        //
        // Scenario:
        // - Batch 1: Hits rows threshold but NOT ratio threshold (low cardinality) -> don't skip
        // - Batch 2: Now hits ratio threshold (high cardinality) -> skip
        //
        // Without the fix, the probe would be locked after batch 1, preventing the skip
        // decision from being made on batch 2.

        let schema = Arc::new(Schema::new(vec![
            Field::new("group_col", DataType::Int32, false),
            Field::new("value_col", DataType::Int32, false),
        ]));

        // Configure thresholds:
        // - probe_rows_threshold: 100 rows
        // - probe_ratio_threshold: 0.8 (80%)
        let probe_rows_threshold = 100;
        let probe_ratio_threshold = 0.8;

        // Batch 1: 100 rows with only 10 unique groups
        // Ratio: 10/100 = 0.1 (10%) < 0.8 -> should NOT skip
        // This will hit the rows threshold but not the ratio threshold
        let batch1_rows = 100;
        let batch1_groups = 10;
        let mut group_ids_batch1 = Vec::new();
        for i in 0..batch1_rows {
            group_ids_batch1.push((i % batch1_groups) as i32);
        }
        let values_batch1: Vec<i32> = vec![1; batch1_rows];

        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(group_ids_batch1)),
                Arc::new(Int32Array::from(values_batch1)),
            ],
        )?;

        // Batch 2: 360 rows with 360 unique NEW groups (starting from group 10)
        // After batch 2, total: 460 rows, 370 groups
        // Ratio: 370/460 is about 0.804 (80.4%) > 0.8 -> SHOULD decide to skip
        let batch2_rows = 360;
        let batch2_groups = 360;
        let group_ids_batch2: Vec<i32> = (batch1_groups..(batch1_groups + batch2_groups))
            .map(|x| x as i32)
            .collect();
        let values_batch2: Vec<i32> = vec![1; batch2_rows];

        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(group_ids_batch2)),
                Arc::new(Int32Array::from(values_batch2)),
            ],
        )?;

        // Batch 3: This batch should be skipped since we decided to skip after batch 2
        // 100 rows with 100 unique groups (continuing from where batch 2 left off)
        let batch3_rows = 100;
        let batch3_groups = 100;
        let batch3_start_group = batch1_groups + batch2_groups;
        let group_ids_batch3: Vec<i32> = (batch3_start_group
            ..(batch3_start_group + batch3_groups))
            .map(|x| x as i32)
            .collect();
        let values_batch3: Vec<i32> = vec![1; batch3_rows];

        let batch3 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(group_ids_batch3)),
                Arc::new(Int32Array::from(values_batch3)),
            ],
        )?;

        let input_partitions = vec![vec![batch1, batch2, batch3]];

        let runtime = RuntimeEnvBuilder::default().build_arc()?;
        let mut task_ctx = TaskContext::default().with_runtime(runtime);

        // Configure skip aggregation settings
        let mut session_config = task_ctx.session_config().clone();
        session_config = session_config.set(
            "datafusion.execution.skip_partial_aggregation_probe_rows_threshold",
            &ScalarValue::UInt64(Some(probe_rows_threshold)),
        );
        session_config = session_config.set(
            "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
            &ScalarValue::Float64(Some(probe_ratio_threshold)),
        );
        task_ctx = task_ctx.with_session_config(session_config);
        let task_ctx = Arc::new(task_ctx);

        // Create aggregate: COUNT(*) GROUP BY group_col
        let group_expr = vec![(col("group_col", &schema)?, "group_col".to_string())];
        let aggr_expr = vec![Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![col("value_col", &schema)?])
                .schema(Arc::clone(&schema))
                .alias("count_value")
                .build()?,
        )];

        let exec = TestMemoryExec::try_new(&input_partitions, Arc::clone(&schema), None)?;
        let exec = Arc::new(TestMemoryExec::update_cache(&Arc::new(exec)));

        // Use Partial mode
        let aggregate_exec = AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(group_expr),
            aggr_expr,
            vec![None],
            exec,
            Arc::clone(&schema),
        )?;

        // Execute and collect results
        let mut stream =
            PartialHashAggregateStream::new(&aggregate_exec, &Arc::clone(&task_ctx), 0)?
                .into_stream();
        let mut results = Vec::new();

        while let Some(result) = stream.next().await {
            let batch = result?;
            results.push(batch);
        }

        // Check that skip aggregation actually happened.
        // The key metric is skipped_aggregation_rows.
        let metrics = aggregate_exec.metrics().unwrap();
        let skipped_rows = metrics
            .sum_by_name("skipped_aggregation_rows")
            .map(|m| m.as_usize())
            .unwrap_or(0);

        // We expect batch 3's rows to be skipped (100 rows)
        assert_eq!(
            skipped_rows, batch3_rows,
            "Expected batch 3's rows ({batch3_rows}) to be skipped",
        );

        Ok(())
    }

    /// Covers the `EmittingOnMemoryPressure` state: every input batch overflows
    /// the memory limit, so the accumulated states are emitted in `batch_size`
    /// slices after each one, and reading resumes with the emptied table.
    #[tokio::test]
    async fn test_partial_hash_stream_emits_early_on_memory_pressure() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("group_col", DataType::UInt32, false),
            Field::new("value_col", DataType::Float64, false),
        ]));
        // Each batch holds `num_groups` distinct groups: far more than the memory
        // limit below can hold, while the emptied table fits comfortably.
        let num_groups: usize = 4096;
        let num_batches = 3;
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(UInt32Array::from_iter_values(0..num_groups as u32)),
                Arc::new(Float64Array::from_iter_values(
                    (0..num_groups).map(|v| v as f64),
                )),
            ],
        )?;
        let input_partitions = vec![vec![batch; num_batches]];

        let runtime = RuntimeEnvBuilder::default()
            .with_memory_limit(16 * 1024, 1.0)
            .build_arc()?;
        let batch_size = 1000;
        let session_config = SessionConfig::new().with_batch_size(batch_size).set(
            // Disable the skip-aggregation probe, so that the memory check is
            // reached after every batch.
            "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
            &ScalarValue::Float64(Some(2.0)),
        );
        let task_ctx = Arc::new(
            TaskContext::default()
                .with_session_config(session_config)
                .with_runtime(runtime),
        );

        let group_expr = vec![(col("group_col", &schema)?, "group_col".to_string())];
        let aggr_expr = vec![Arc::new(
            AggregateExprBuilder::new(sum_udaf(), vec![col("value_col", &schema)?])
                .schema(Arc::clone(&schema))
                .alias("sum_value")
                .build()?,
        )];
        let exec =
            TestMemoryExec::try_new_exec(&input_partitions, Arc::clone(&schema), None)?;
        let aggregate_exec = AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(group_expr),
            aggr_expr,
            vec![None],
            exec,
            Arc::clone(&schema),
        )?;

        let stream =
            PartialHashAggregateStream::new(&aggregate_exec, &task_ctx, 0)?.into_stream();
        let output = collect(stream).await?;

        // Each input batch is flushed as one run of `num_groups` states, sliced
        // into `batch_size` output batches.
        let expected_sizes: Vec<usize> = (0..num_batches)
            .flat_map(|_| {
                (0..num_groups)
                    .step_by(batch_size)
                    .map(|offset| batch_size.min(num_groups - offset))
            })
            .collect();
        assert_eq!(
            output.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
            expected_sizes
        );

        // Every group's partial sum is emitted once per flush, so the output sums
        // add up to the input sum once per input batch.
        let total: f64 = output
            .iter()
            .map(|batch| {
                let sums = batch.column(1).as_primitive::<Float64Type>();
                sums.values().iter().sum::<f64>()
            })
            .sum();
        let expected_total =
            num_batches as f64 * (0..num_groups).map(|v| v as f64).sum::<f64>();
        assert_eq!(total, expected_total);

        // Output rows are recorded once by the `ObservedStream` wrapper in
        // `into_stream`, so every early-emitted slice must be counted exactly once.
        let metrics = aggregate_exec.metrics().unwrap();
        assert_eq!(metrics.output_rows(), Some(num_batches * num_groups));

        Ok(())
    }

    /// An input error ends the stream right away, and releases the memory
    /// reservation held by the generator together with the hash table.
    #[tokio::test]
    async fn test_partial_hash_stream_input_error_ends_stream() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("group_col", DataType::Int32, false),
            Field::new("value_col", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 1, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            ],
        )?;
        // One batch is aggregated, and reserves memory, before the input fails.
        let input = MockExec::new(
            vec![Ok(batch), exec_err!("planted input error")],
            Arc::clone(&schema),
        )
        .with_unknown_statistics();

        let group_expr = vec![(col("group_col", &schema)?, "group_col".to_string())];
        let aggr_expr = vec![Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![col("value_col", &schema)?])
                .schema(Arc::clone(&schema))
                .alias("count_value")
                .build()?,
        )];
        let aggregate_exec = AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(group_expr),
            aggr_expr,
            vec![None],
            Arc::new(input),
            Arc::clone(&schema),
        )?;
        let task_ctx = Arc::new(TaskContext::default());

        let mut stream =
            PartialHashAggregateStream::new(&aggregate_exec, &task_ctx, 0)?.into_stream();
        let err = stream
            .next()
            .await
            .expect("the input error is yielded")
            .unwrap_err();
        assert_contains!(err.to_string(), "planted input error");
        assert!(
            stream.next().await.is_none(),
            "the stream ends after the error"
        );
        assert_eq!(
            task_ctx.memory_pool().reserved(),
            0,
            "the reservation is released after the error"
        );

        Ok(())
    }
}
