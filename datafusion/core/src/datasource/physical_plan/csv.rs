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

//! Execution plan for reading CSV files

use std::any::Any;
use std::fmt;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;
use std::task::Poll;

use super::{calculate_range, FileScanConfig, RangeCalculation};
use crate::datasource::data_source::FileSource;
use crate::datasource::file_format::file_compression_type::FileCompressionType;
use crate::datasource::file_format::{deserialize_stream, DecoderDeserializer};
use crate::datasource::listing::{FileRange, ListingTableUrl, PartitionedFile};
use crate::datasource::physical_plan::FileMeta;
use crate::datasource::physical_plan::{FileOpenFuture, FileOpener};
use crate::error::{DataFusionError, Result};
use crate::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use arrow::csv;
use arrow::datatypes::SchemaRef;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{Constraints, Statistics};
use datafusion_datasource::source::DataSourceExec;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, PlanProperties};

use futures::{StreamExt, TryStreamExt};
use object_store::buffered::BufWriter;
use object_store::{GetOptions, GetResultPayload, ObjectStore};
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;

/// Old Csv source, deprecated with DataSourceExec implementation and CsvSource
///
/// See examples on `CsvSource`
#[derive(Debug, Clone)]
#[deprecated(since = "46.0.0", note = "use DataSourceExec instead")]
pub struct CsvExec {
    base_config: FileScanConfig,
    inner: DataSourceExec,
}

/// Builder for [`CsvExec`].
///
/// See example on [`CsvExec`].
#[derive(Debug, Clone)]
#[deprecated(since = "46.0.0", note = "use FileScanConfig instead")]
pub struct CsvExecBuilder {
    file_scan_config: FileScanConfig,
    file_compression_type: FileCompressionType,
    // TODO: it seems like these format options could be reused across all the various CSV config
    has_header: bool,
    delimiter: u8,
    quote: u8,
    terminator: Option<u8>,
    escape: Option<u8>,
    comment: Option<u8>,
    newlines_in_values: bool,
}

#[allow(unused, deprecated)]
impl CsvExecBuilder {
    /// Create a new builder to read the provided file scan configuration.
    pub fn new(file_scan_config: FileScanConfig) -> Self {
        Self {
            file_scan_config,
            // TODO: these defaults are duplicated from `CsvOptions` - should they be computed?
            has_header: false,
            delimiter: b',',
            quote: b'"',
            terminator: None,
            escape: None,
            comment: None,
            newlines_in_values: false,
            file_compression_type: FileCompressionType::UNCOMPRESSED,
        }
    }

    /// Set whether the first row defines the column names.
    ///
    /// The default value is `false`.
    pub fn with_has_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }

    /// Set the column delimeter.
    ///
    /// The default is `,`.
    pub fn with_delimeter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    /// Set the quote character.
    ///
    /// The default is `"`.
    pub fn with_quote(mut self, quote: u8) -> Self {
        self.quote = quote;
        self
    }

    /// Set the line terminator. If not set, the default is CRLF.
    ///
    /// The default is None.
    pub fn with_terminator(mut self, terminator: Option<u8>) -> Self {
        self.terminator = terminator;
        self
    }

    /// Set the escape character.
    ///
    /// The default is `None` (i.e. quotes cannot be escaped).
    pub fn with_escape(mut self, escape: Option<u8>) -> Self {
        self.escape = escape;
        self
    }

    /// Set the comment character.
    ///
    /// The default is `None` (i.e. comments are not supported).
    pub fn with_comment(mut self, comment: Option<u8>) -> Self {
        self.comment = comment;
        self
    }

    /// Set whether newlines in (quoted) values are supported.
    ///
    /// Parsing newlines in quoted values may be affected by execution behaviour such as
    /// parallel file scanning. Setting this to `true` ensures that newlines in values are
    /// parsed successfully, which may reduce performance.
    ///
    /// The default value is `false`.
    pub fn with_newlines_in_values(mut self, newlines_in_values: bool) -> Self {
        self.newlines_in_values = newlines_in_values;
        self
    }

    /// Set the file compression type.
    ///
    /// The default is [`FileCompressionType::UNCOMPRESSED`].
    pub fn with_file_compression_type(
        mut self,
        file_compression_type: FileCompressionType,
    ) -> Self {
        self.file_compression_type = file_compression_type;
        self
    }

    /// Build a [`CsvExec`].
    #[must_use]
    pub fn build(self) -> CsvExec {
        let Self {
            file_scan_config: base_config,
            file_compression_type,
            has_header,
            delimiter,
            quote,
            terminator,
            escape,
            comment,
            newlines_in_values,
        } = self;

        let (
            projected_schema,
            projected_constraints,
            projected_statistics,
            projected_output_ordering,
        ) = base_config.project();
        let cache = CsvExec::compute_properties(
            projected_schema,
            &projected_output_ordering,
            projected_constraints,
            &base_config,
        );
        let csv = CsvSource::new(has_header, delimiter, quote)
            .with_comment(comment)
            .with_escape(escape)
            .with_terminator(terminator);
        let base_config = base_config
            .with_newlines_in_values(newlines_in_values)
            .with_file_compression_type(file_compression_type)
            .with_source(Arc::new(csv));

        CsvExec {
            inner: DataSourceExec::new(Arc::new(base_config.clone())),
            base_config,
        }
    }
}

#[allow(unused, deprecated)]
impl CsvExec {
    /// Create a new CSV reader execution plan provided base and specific configurations
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        base_config: FileScanConfig,
        has_header: bool,
        delimiter: u8,
        quote: u8,
        terminator: Option<u8>,
        escape: Option<u8>,
        comment: Option<u8>,
        newlines_in_values: bool,
        file_compression_type: FileCompressionType,
    ) -> Self {
        CsvExecBuilder::new(base_config)
            .with_has_header(has_header)
            .with_delimeter(delimiter)
            .with_quote(quote)
            .with_terminator(terminator)
            .with_escape(escape)
            .with_comment(comment)
            .with_newlines_in_values(newlines_in_values)
            .with_file_compression_type(file_compression_type)
            .build()
    }

    /// Return a [`CsvExecBuilder`].
    ///
    /// See example on [`CsvExec`] and [`CsvExecBuilder`] for specifying CSV table options.
    pub fn builder(file_scan_config: FileScanConfig) -> CsvExecBuilder {
        CsvExecBuilder::new(file_scan_config)
    }

    /// Ref to the base configs
    pub fn base_config(&self) -> &FileScanConfig {
        &self.base_config
    }

    fn file_scan_config(&self) -> FileScanConfig {
        let source = self.inner.source();
        source
            .as_any()
            .downcast_ref::<FileScanConfig>()
            .unwrap()
            .clone()
    }

    fn csv_source(&self) -> CsvSource {
        let source = self.file_scan_config();
        source
            .file_source()
            .as_any()
            .downcast_ref::<CsvSource>()
            .unwrap()
            .clone()
    }

    /// true if the first line of each file is a header
    pub fn has_header(&self) -> bool {
        self.csv_source().has_header()
    }

    /// Specifies whether newlines in (quoted) values are supported.
    ///
    /// Parsing newlines in quoted values may be affected by execution behaviour such as
    /// parallel file scanning. Setting this to `true` ensures that newlines in values are
    /// parsed successfully, which may reduce performance.
    ///
    /// The default behaviour depends on the `datafusion.catalog.newlines_in_values` setting.
    pub fn newlines_in_values(&self) -> bool {
        let source = self.file_scan_config();
        source.newlines_in_values()
    }

    fn output_partitioning_helper(file_scan_config: &FileScanConfig) -> Partitioning {
        Partitioning::UnknownPartitioning(file_scan_config.file_groups.len())
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        schema: SchemaRef,
        orderings: &[LexOrdering],
        constraints: Constraints,
        file_scan_config: &FileScanConfig,
    ) -> PlanProperties {
        // Equivalence Properties
        let eq_properties = EquivalenceProperties::new_with_orderings(schema, orderings)
            .with_constraints(constraints);

        PlanProperties::new(
            eq_properties,
            Self::output_partitioning_helper(file_scan_config), // Output Partitioning
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }

    fn with_file_groups(mut self, file_groups: Vec<Vec<PartitionedFile>>) -> Self {
        self.base_config.file_groups = file_groups.clone();
        let mut file_source = self.file_scan_config();
        file_source = file_source.with_file_groups(file_groups);
        self.inner = self.inner.with_source(Arc::new(file_source));
        self
    }
}

#[allow(unused, deprecated)]
impl DisplayAs for CsvExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        self.inner.fmt_as(t, f)
    }
}

#[allow(unused, deprecated)]
impl ExecutionPlan for CsvExec {
    fn name(&self) -> &'static str {
        "CsvExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    /// Redistribute files across partitions according to their size
    /// See comments on `FileGroupPartitioner` for more detail.
    ///
    /// Return `None` if can't get repartitioned (empty, compressed file, or `newlines_in_values` set).
    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        self.inner.repartitioned(target_partitions, config)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }

    fn statistics(&self) -> Result<Statistics> {
        self.inner.statistics()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }

    fn fetch(&self) -> Option<usize> {
        self.inner.fetch()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        self.inner.with_fetch(limit)
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        self.inner.try_swapping_with_projection(projection)
    }
}

/// A Config for [`CsvOpener`]
///
/// # Example: create a `DataSourceExec` for CSV
/// ```
/// # use std::sync::Arc;
/// # use arrow::datatypes::Schema;
/// # use datafusion::datasource::{
/// #     physical_plan::FileScanConfig,
/// #     listing::PartitionedFile,
/// # };
/// # use datafusion::datasource::physical_plan::CsvSource;
/// # use datafusion_execution::object_store::ObjectStoreUrl;
/// # use datafusion::datasource::source::DataSourceExec;
///
/// # let object_store_url = ObjectStoreUrl::local_filesystem();
/// # let file_schema = Arc::new(Schema::empty());
///
/// let source = Arc::new(CsvSource::new(
///         true,
///         b',',
///         b'"',
///     )
///     .with_terminator(Some(b'#')
/// ));
/// // Create a DataSourceExec for reading the first 100MB of `file1.csv`
/// let file_scan_config = FileScanConfig::new(object_store_url, file_schema, source)
///     .with_file(PartitionedFile::new("file1.csv", 100*1024*1024))
///     .with_newlines_in_values(true); // The file contains newlines in values;
/// let exec = file_scan_config.build();
/// ```
#[derive(Debug, Clone, Default)]
pub struct CsvSource {
    batch_size: Option<usize>,
    file_schema: Option<SchemaRef>,
    file_projection: Option<Vec<usize>>,
    pub(crate) has_header: bool,
    delimiter: u8,
    quote: u8,
    terminator: Option<u8>,
    escape: Option<u8>,
    comment: Option<u8>,
    metrics: ExecutionPlanMetricsSet,
    projected_statistics: Option<Statistics>,
}

impl CsvSource {
    /// Returns a [`CsvSource`]
    pub fn new(has_header: bool, delimiter: u8, quote: u8) -> Self {
        Self {
            has_header,
            delimiter,
            quote,
            ..Self::default()
        }
    }

    /// true if the first line of each file is a header
    pub fn has_header(&self) -> bool {
        self.has_header
    }
    /// A column delimiter
    pub fn delimiter(&self) -> u8 {
        self.delimiter
    }

    /// The quote character
    pub fn quote(&self) -> u8 {
        self.quote
    }

    /// The line terminator
    pub fn terminator(&self) -> Option<u8> {
        self.terminator
    }

    /// Lines beginning with this byte are ignored.
    pub fn comment(&self) -> Option<u8> {
        self.comment
    }

    /// The escape character
    pub fn escape(&self) -> Option<u8> {
        self.escape
    }

    /// Initialize a CsvSource with escape
    pub fn with_escape(&self, escape: Option<u8>) -> Self {
        let mut conf = self.clone();
        conf.escape = escape;
        conf
    }

    /// Initialize a CsvSource with terminator
    pub fn with_terminator(&self, terminator: Option<u8>) -> Self {
        let mut conf = self.clone();
        conf.terminator = terminator;
        conf
    }

    /// Initialize a CsvSource with comment
    pub fn with_comment(&self, comment: Option<u8>) -> Self {
        let mut conf = self.clone();
        conf.comment = comment;
        conf
    }
}

impl CsvSource {
    fn open<R: Read>(&self, reader: R) -> Result<csv::Reader<R>> {
        Ok(self.builder().build(reader)?)
    }

    fn builder(&self) -> csv::ReaderBuilder {
        let mut builder = csv::ReaderBuilder::new(Arc::clone(
            self.file_schema
                .as_ref()
                .expect("Schema must be set before initializing builder"),
        ))
        .with_delimiter(self.delimiter)
        .with_batch_size(
            self.batch_size
                .expect("Batch size must be set before initializing builder"),
        )
        .with_header(self.has_header)
        .with_quote(self.quote);
        if let Some(terminator) = self.terminator {
            builder = builder.with_terminator(terminator);
        }
        if let Some(proj) = &self.file_projection {
            builder = builder.with_projection(proj.clone());
        }
        if let Some(escape) = self.escape {
            builder = builder.with_escape(escape)
        }
        if let Some(comment) = self.comment {
            builder = builder.with_comment(comment);
        }

        builder
    }
}

/// A [`FileOpener`] that opens a CSV file and yields a [`FileOpenFuture`]
pub struct CsvOpener {
    config: Arc<CsvSource>,
    file_compression_type: FileCompressionType,
    object_store: Arc<dyn ObjectStore>,
}

impl CsvOpener {
    /// Returns a [`CsvOpener`]
    pub fn new(
        config: Arc<CsvSource>,
        file_compression_type: FileCompressionType,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            config,
            file_compression_type,
            object_store,
        }
    }
}

impl FileSource for CsvSource {
    fn create_file_opener(
        &self,
        object_store: Result<Arc<dyn ObjectStore>>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        Ok(Arc::new(CsvOpener {
            config: Arc::new(self.clone()),
            file_compression_type: base_config.file_compression_type,
            object_store: object_store?,
        }))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.batch_size = Some(batch_size);
        Arc::new(conf)
    }

    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.file_schema = Some(schema);
        Arc::new(conf)
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.projected_statistics = Some(statistics);
        Arc::new(conf)
    }

    fn with_projection(&self, config: &FileScanConfig) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.file_projection = config.file_column_projection_indices();
        Arc::new(conf)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }
    fn statistics(&self) -> Result<Statistics> {
        let statistics = &self.projected_statistics;
        Ok(statistics
            .clone()
            .expect("projected_statistics must be set"))
    }
    fn file_type(&self) -> &str {
        "csv"
    }
    fn fmt_extra(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, ", has_header={}", self.has_header)
    }
}

impl FileOpener for CsvOpener {
    /// Open a partitioned CSV file.
    ///
    /// If `file_meta.range` is `None`, the entire file is opened.
    /// If `file_meta.range` is `Some(FileRange {start, end})`, this signifies that the partition
    /// corresponds to the byte range [start, end) within the file.
    ///
    /// Note: `start` or `end` might be in the middle of some lines. In such cases, the following rules
    /// are applied to determine which lines to read:
    /// 1. The first line of the partition is the line in which the index of the first character >= `start`.
    /// 2. The last line of the partition is the line in which the byte at position `end - 1` resides.
    ///
    /// Examples:
    /// Consider the following partitions enclosed by braces `{}`:
    ///
    /// {A,1,2,3,4,5,6,7,8,9\n
    ///  A,1,2,3,4,5,6,7,8,9\n}
    ///  A,1,2,3,4,5,6,7,8,9\n
    ///  The lines read would be: [0, 1]
    ///
    ///  A,{1,2,3,4,5,6,7,8,9\n
    ///  A,1,2,3,4,5,6,7,8,9\n
    ///  A},1,2,3,4,5,6,7,8,9\n
    ///  The lines read would be: [1, 2]
    fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture> {
        // `self.config.has_header` controls whether to skip reading the 1st line header
        // If the .csv file is read in parallel and this `CsvOpener` is only reading some middle
        // partition, then don't skip first line
        let mut csv_has_header = self.config.has_header;
        if let Some(FileRange { start, .. }) = file_meta.range {
            if start != 0 {
                csv_has_header = false;
            }
        }

        let config = CsvSource {
            has_header: csv_has_header,
            ..(*self.config).clone()
        };

        let file_compression_type = self.file_compression_type.to_owned();

        if file_meta.range.is_some() {
            assert!(
                !file_compression_type.is_compressed(),
                "Reading compressed .csv in parallel is not supported"
            );
        }

        let store = Arc::clone(&self.object_store);
        let terminator = self.config.terminator;

        Ok(Box::pin(async move {
            // Current partition contains bytes [start_byte, end_byte) (might contain incomplete lines at boundaries)

            let calculated_range =
                calculate_range(&file_meta, &store, terminator).await?;

            let range = match calculated_range {
                RangeCalculation::Range(None) => None,
                RangeCalculation::Range(Some(range)) => Some(range.into()),
                RangeCalculation::TerminateEarly => {
                    return Ok(
                        futures::stream::poll_fn(move |_| Poll::Ready(None)).boxed()
                    )
                }
            };

            let options = GetOptions {
                range,
                ..Default::default()
            };

            let result = store.get_opts(file_meta.location(), options).await?;

            match result.payload {
                GetResultPayload::File(mut file, _) => {
                    let is_whole_file_scanned = file_meta.range.is_none();
                    let decoder = if is_whole_file_scanned {
                        // Don't seek if no range as breaks FIFO files
                        file_compression_type.convert_read(file)?
                    } else {
                        file.seek(SeekFrom::Start(result.range.start as _))?;
                        file_compression_type.convert_read(
                            file.take((result.range.end - result.range.start) as u64),
                        )?
                    };

                    Ok(futures::stream::iter(config.open(decoder)?).boxed())
                }
                GetResultPayload::Stream(s) => {
                    let decoder = config.builder().build_decoder();
                    let s = s.map_err(DataFusionError::from);
                    let input = file_compression_type.convert_stream(s.boxed())?.fuse();

                    Ok(deserialize_stream(
                        input,
                        DecoderDeserializer::from(decoder),
                    ))
                }
            }
        }))
    }
}

pub async fn plan_to_csv(
    task_ctx: Arc<TaskContext>,
    plan: Arc<dyn ExecutionPlan>,
    path: impl AsRef<str>,
) -> Result<()> {
    let path = path.as_ref();
    let parsed = ListingTableUrl::parse(path)?;
    let object_store_url = parsed.object_store();
    let store = task_ctx.runtime_env().object_store(&object_store_url)?;
    let mut join_set = JoinSet::new();
    for i in 0..plan.output_partitioning().partition_count() {
        let storeref = Arc::clone(&store);
        let plan: Arc<dyn ExecutionPlan> = Arc::clone(&plan);
        let filename = format!("{}/part-{i}.csv", parsed.prefix());
        let file = object_store::path::Path::parse(filename)?;

        let mut stream = plan.execute(i, Arc::clone(&task_ctx))?;
        join_set.spawn(async move {
            let mut buf_writer = BufWriter::new(storeref, file.clone());
            let mut buffer = Vec::with_capacity(1024);
            //only write headers on first iteration
            let mut write_headers = true;
            while let Some(batch) = stream.next().await.transpose()? {
                let mut writer = csv::WriterBuilder::new()
                    .with_header(write_headers)
                    .build(buffer);
                writer.write(&batch)?;
                buffer = writer.into_inner();
                buf_writer.write_all(&buffer).await?;
                buffer.clear();
                //prevent writing headers more than once
                write_headers = false;
            }
            buf_writer.shutdown().await.map_err(DataFusionError::from)
        });
    }

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(res) => res?, // propagate DataFusion error
            Err(e) => {
                if e.is_panic() {
                    std::panic::resume_unwind(e.into_panic());
                } else {
                    unreachable!();
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::io::Write;

    use super::*;
    use crate::dataframe::DataFrameWriteOptions;
    use crate::datasource::file_format::csv::CsvFormat;
    use crate::prelude::*;
    use crate::test::{partitioned_csv_config, partitioned_file_groups};
    use crate::{scalar::ScalarValue, test_util::aggr_test_schema};

    use arrow::datatypes::*;
    use bytes::Bytes;
    use datafusion_common::test_util::arrow_test_data;
    use datafusion_physical_plan::metrics::MetricsSet;

    use object_store::chunked::ChunkedStore;
    use object_store::local::LocalFileSystem;
    use rstest::*;
    use tempfile::TempDir;
    use url::Url;

    #[rstest(
        file_compression_type,
        case(FileCompressionType::UNCOMPRESSED),
        case(FileCompressionType::GZIP),
        case(FileCompressionType::BZIP2),
        case(FileCompressionType::XZ),
        case(FileCompressionType::ZSTD)
    )]
    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn csv_exec_with_projection(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        use crate::datasource::file_format::csv::CsvFormat;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let file_schema = aggr_test_schema();
        let path = format!("{}/csv", arrow_test_data());
        let filename = "aggregate_test_100.csv";
        let tmp_dir = TempDir::new()?;

        let file_groups = partitioned_file_groups(
            path.as_str(),
            filename,
            1,
            Arc::new(CsvFormat::default()),
            file_compression_type.to_owned(),
            tmp_dir.path(),
        )?;

        let source = Arc::new(CsvSource::new(true, b',', b'"'));
        let config = partitioned_csv_config(file_schema, file_groups, source)
            .with_file_compression_type(file_compression_type)
            .with_newlines_in_values(false)
            .with_projection(Some(vec![0, 2, 4]));

        assert_eq!(13, config.file_schema.fields().len());
        let csv = config.build();

        assert_eq!(3, csv.schema().fields().len());

        let mut stream = csv.execute(0, task_ctx)?;
        let batch = stream.next().await.unwrap()?;
        assert_eq!(3, batch.num_columns());
        assert_eq!(100, batch.num_rows());

        // slice of the first 5 lines
        let expected = [
            "+----+-----+------------+",
            "| c1 | c3  | c5         |",
            "+----+-----+------------+",
            "| c  | 1   | 2033001162 |",
            "| d  | -40 | 706441268  |",
            "| b  | 29  | 994303988  |",
            "| a  | -85 | 1171968280 |",
            "| b  | -82 | 1824882165 |",
            "+----+-----+------------+",
        ];

        crate::assert_batches_eq!(expected, &[batch.slice(0, 5)]);
        Ok(())
    }

    #[rstest(
        file_compression_type,
        case(FileCompressionType::UNCOMPRESSED),
        case(FileCompressionType::GZIP),
        case(FileCompressionType::BZIP2),
        case(FileCompressionType::XZ),
        case(FileCompressionType::ZSTD)
    )]
    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn csv_exec_with_mixed_order_projection(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        use crate::datasource::file_format::csv::CsvFormat;

        let cfg = SessionConfig::new().set_str("datafusion.catalog.has_header", "true");
        let session_ctx = SessionContext::new_with_config(cfg);
        let task_ctx = session_ctx.task_ctx();
        let file_schema = aggr_test_schema();
        let path = format!("{}/csv", arrow_test_data());
        let filename = "aggregate_test_100.csv";
        let tmp_dir = TempDir::new()?;

        let file_groups = partitioned_file_groups(
            path.as_str(),
            filename,
            1,
            Arc::new(CsvFormat::default()),
            file_compression_type.to_owned(),
            tmp_dir.path(),
        )?;

        let source = Arc::new(CsvSource::new(true, b',', b'"'));
        let config = partitioned_csv_config(file_schema, file_groups, source)
            .with_newlines_in_values(false)
            .with_file_compression_type(file_compression_type.to_owned())
            .with_projection(Some(vec![4, 0, 2]));
        assert_eq!(13, config.file_schema.fields().len());
        let csv = config.build();
        assert_eq!(3, csv.schema().fields().len());

        let mut stream = csv.execute(0, task_ctx)?;
        let batch = stream.next().await.unwrap()?;
        assert_eq!(3, batch.num_columns());
        assert_eq!(100, batch.num_rows());

        // slice of the first 5 lines
        let expected = [
            "+------------+----+-----+",
            "| c5         | c1 | c3  |",
            "+------------+----+-----+",
            "| 2033001162 | c  | 1   |",
            "| 706441268  | d  | -40 |",
            "| 994303988  | b  | 29  |",
            "| 1171968280 | a  | -85 |",
            "| 1824882165 | b  | -82 |",
            "+------------+----+-----+",
        ];

        crate::assert_batches_eq!(expected, &[batch.slice(0, 5)]);
        Ok(())
    }

    #[rstest(
        file_compression_type,
        case(FileCompressionType::UNCOMPRESSED),
        case(FileCompressionType::GZIP),
        case(FileCompressionType::BZIP2),
        case(FileCompressionType::XZ),
        case(FileCompressionType::ZSTD)
    )]
    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn csv_exec_with_limit(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        use crate::datasource::file_format::csv::CsvFormat;

        let cfg = SessionConfig::new().set_str("datafusion.catalog.has_header", "true");
        let session_ctx = SessionContext::new_with_config(cfg);
        let task_ctx = session_ctx.task_ctx();
        let file_schema = aggr_test_schema();
        let path = format!("{}/csv", arrow_test_data());
        let filename = "aggregate_test_100.csv";
        let tmp_dir = TempDir::new()?;

        let file_groups = partitioned_file_groups(
            path.as_str(),
            filename,
            1,
            Arc::new(CsvFormat::default()),
            file_compression_type.to_owned(),
            tmp_dir.path(),
        )?;

        let source = Arc::new(CsvSource::new(true, b',', b'"'));
        let config = partitioned_csv_config(file_schema, file_groups, source)
            .with_newlines_in_values(false)
            .with_file_compression_type(file_compression_type.to_owned())
            .with_limit(Some(5));
        assert_eq!(13, config.file_schema.fields().len());
        let csv = config.build();
        assert_eq!(13, csv.schema().fields().len());

        let mut it = csv.execute(0, task_ctx)?;
        let batch = it.next().await.unwrap()?;
        assert_eq!(13, batch.num_columns());
        assert_eq!(5, batch.num_rows());

        let expected = ["+----+----+-----+--------+------------+----------------------+-----+-------+------------+----------------------+-------------+---------------------+--------------------------------+",
            "| c1 | c2 | c3  | c4     | c5         | c6                   | c7  | c8    | c9         | c10                  | c11         | c12                 | c13                            |",
            "+----+----+-----+--------+------------+----------------------+-----+-------+------------+----------------------+-------------+---------------------+--------------------------------+",
            "| c  | 2  | 1   | 18109  | 2033001162 | -6513304855495910254 | 25  | 43062 | 1491205016 | 5863949479783605708  | 0.110830784 | 0.9294097332465232  | 6WfVFBVGJSQb7FhA7E0lBwdvjfZnSW |",
            "| d  | 5  | -40 | 22614  | 706441268  | -7542719935673075327 | 155 | 14337 | 3373581039 | 11720144131976083864 | 0.69632107  | 0.3114712539863804  | C2GT5KVyOPZpgKVl110TyZO0NcJ434 |",
            "| b  | 1  | 29  | -18218 | 994303988  | 5983957848665088916  | 204 | 9489  | 3275293996 | 14857091259186476033 | 0.53840446  | 0.17909035118828576 | AyYVExXK6AR2qUTxNZ7qRHQOVGMLcz |",
            "| a  | 1  | -85 | -15154 | 1171968280 | 1919439543497968449  | 77  | 52286 | 774637006  | 12101411955859039553 | 0.12285209  | 0.6864391962767343  | 0keZ5G8BffGwgF2RwQD59TFzMStxCB |",
            "| b  | 5  | -82 | 22080  | 1824882165 | 7373730676428214987  | 208 | 34331 | 3342719438 | 3330177516592499461  | 0.82634634  | 0.40975383525297016 | Ig1QcuKsjHXkproePdERo2w0mYzIqd |",
            "+----+----+-----+--------+------------+----------------------+-----+-------+------------+----------------------+-------------+---------------------+--------------------------------+"];

        crate::assert_batches_eq!(expected, &[batch]);

        Ok(())
    }

    #[rstest(
        file_compression_type,
        case(FileCompressionType::UNCOMPRESSED),
        case(FileCompressionType::GZIP),
        case(FileCompressionType::BZIP2),
        case(FileCompressionType::XZ),
        case(FileCompressionType::ZSTD)
    )]
    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn csv_exec_with_missing_column(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        use crate::datasource::file_format::csv::CsvFormat;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let file_schema = aggr_test_schema_with_missing_col();
        let path = format!("{}/csv", arrow_test_data());
        let filename = "aggregate_test_100.csv";
        let tmp_dir = TempDir::new()?;

        let file_groups = partitioned_file_groups(
            path.as_str(),
            filename,
            1,
            Arc::new(CsvFormat::default()),
            file_compression_type.to_owned(),
            tmp_dir.path(),
        )?;

        let source = Arc::new(CsvSource::new(true, b',', b'"'));
        let config = partitioned_csv_config(file_schema, file_groups, source)
            .with_newlines_in_values(false)
            .with_file_compression_type(file_compression_type.to_owned())
            .with_limit(Some(5));
        assert_eq!(14, config.file_schema.fields().len());
        let csv = config.build();
        assert_eq!(14, csv.schema().fields().len());

        // errors due to https://github.com/apache/datafusion/issues/4918
        let mut it = csv.execute(0, task_ctx)?;
        let err = it.next().await.unwrap().unwrap_err().strip_backtrace();
        assert_eq!(
            err,
            "Arrow error: Csv error: incorrect number of fields for line 1, expected 14 got 13"
        );
        Ok(())
    }

    #[rstest(
        file_compression_type,
        case(FileCompressionType::UNCOMPRESSED),
        case(FileCompressionType::GZIP),
        case(FileCompressionType::BZIP2),
        case(FileCompressionType::XZ),
        case(FileCompressionType::ZSTD)
    )]
    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn csv_exec_with_partition(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        use crate::datasource::file_format::csv::CsvFormat;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let file_schema = aggr_test_schema();
        let path = format!("{}/csv", arrow_test_data());
        let filename = "aggregate_test_100.csv";
        let tmp_dir = TempDir::new()?;

        let file_groups = partitioned_file_groups(
            path.as_str(),
            filename,
            1,
            Arc::new(CsvFormat::default()),
            file_compression_type.to_owned(),
            tmp_dir.path(),
        )?;

        let source = Arc::new(CsvSource::new(true, b',', b'"'));
        let mut config = partitioned_csv_config(file_schema, file_groups, source)
            .with_newlines_in_values(false)
            .with_file_compression_type(file_compression_type.to_owned());

        // Add partition columns
        config.table_partition_cols = vec![Field::new("date", DataType::Utf8, false)];
        config.file_groups[0][0].partition_values = vec![ScalarValue::from("2021-10-26")];

        // We should be able to project on the partition column
        // Which is supposed to be after the file fields
        config.projection = Some(vec![0, config.file_schema.fields().len()]);

        // we don't have `/date=xx/` in the path but that is ok because
        // partitions are resolved during scan anyway

        assert_eq!(13, config.file_schema.fields().len());
        let csv = config.build();
        assert_eq!(2, csv.schema().fields().len());

        let mut it = csv.execute(0, task_ctx)?;
        let batch = it.next().await.unwrap()?;
        assert_eq!(2, batch.num_columns());
        assert_eq!(100, batch.num_rows());

        // slice of the first 5 lines
        let expected = [
            "+----+------------+",
            "| c1 | date       |",
            "+----+------------+",
            "| c  | 2021-10-26 |",
            "| d  | 2021-10-26 |",
            "| b  | 2021-10-26 |",
            "| a  | 2021-10-26 |",
            "| b  | 2021-10-26 |",
            "+----+------------+",
        ];
        crate::assert_batches_eq!(expected, &[batch.slice(0, 5)]);

        let metrics = csv.metrics().expect("doesn't found metrics");
        let time_elapsed_processing = get_value(&metrics, "time_elapsed_processing");
        assert!(
            time_elapsed_processing > 0,
            "Expected time_elapsed_processing greater than 0",
        );
        Ok(())
    }

    /// Generate CSV partitions within the supplied directory
    fn populate_csv_partitions(
        tmp_dir: &TempDir,
        partition_count: usize,
        file_extension: &str,
    ) -> Result<SchemaRef> {
        // define schema for data source (csv file)
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::UInt32, false),
            Field::new("c2", DataType::UInt64, false),
            Field::new("c3", DataType::Boolean, false),
        ]));

        // generate a partitioned file
        for partition in 0..partition_count {
            let filename = format!("partition-{partition}.{file_extension}");
            let file_path = tmp_dir.path().join(filename);
            let mut file = File::create(file_path)?;

            // generate some data
            for i in 0..=10 {
                let data = format!("{},{},{}\n", partition, i, i % 2 == 0);
                file.write_all(data.as_bytes())?;
            }
        }

        Ok(schema)
    }

    async fn test_additional_stores(
        file_compression_type: FileCompressionType,
        store: Arc<dyn ObjectStore>,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        let url = Url::parse("file://").unwrap();
        ctx.register_object_store(&url, store.clone());

        let task_ctx = ctx.task_ctx();

        let file_schema = aggr_test_schema();
        let path = format!("{}/csv", arrow_test_data());
        let filename = "aggregate_test_100.csv";
        let tmp_dir = TempDir::new()?;

        let file_groups = partitioned_file_groups(
            path.as_str(),
            filename,
            1,
            Arc::new(CsvFormat::default()),
            file_compression_type.to_owned(),
            tmp_dir.path(),
        )
        .unwrap();

        let source = Arc::new(CsvSource::new(true, b',', b'"'));
        let config = partitioned_csv_config(file_schema, file_groups, source)
            .with_newlines_in_values(false)
            .with_file_compression_type(file_compression_type.to_owned());
        let csv = config.build();

        let it = csv.execute(0, task_ctx).unwrap();
        let batches: Vec<_> = it.try_collect().await.unwrap();

        let total_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();

        assert_eq!(total_rows, 100);
        Ok(())
    }

    #[rstest(
        file_compression_type,
        case(FileCompressionType::UNCOMPRESSED),
        case(FileCompressionType::GZIP),
        case(FileCompressionType::BZIP2),
        case(FileCompressionType::XZ),
        case(FileCompressionType::ZSTD)
    )]
    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn test_chunked_csv(
        file_compression_type: FileCompressionType,
        #[values(10, 20, 30, 40)] chunk_size: usize,
    ) -> Result<()> {
        test_additional_stores(
            file_compression_type,
            Arc::new(ChunkedStore::new(
                Arc::new(LocalFileSystem::new()),
                chunk_size,
            )),
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_no_trailing_delimiter() {
        let session_ctx = SessionContext::new();
        let store = object_store::memory::InMemory::new();

        let data = Bytes::from("a,b\n1,2\n3,4");
        let path = object_store::path::Path::from("a.csv");
        store.put(&path, data.into()).await.unwrap();

        let url = Url::parse("memory://").unwrap();
        session_ctx.register_object_store(&url, Arc::new(store));

        let df = session_ctx
            .read_csv("memory:///", CsvReadOptions::new())
            .await
            .unwrap();

        let result = df.collect().await.unwrap();

        let expected = [
            "+---+---+",
            "| a | b |",
            "+---+---+",
            "| 1 | 2 |",
            "| 3 | 4 |",
            "+---+---+",
        ];

        crate::assert_batches_eq!(expected, &result);
    }

    #[tokio::test]
    async fn test_terminator() {
        let session_ctx = SessionContext::new();
        let store = object_store::memory::InMemory::new();

        let data = Bytes::from("a,b\r1,2\r3,4");
        let path = object_store::path::Path::from("a.csv");
        store.put(&path, data.into()).await.unwrap();

        let url = Url::parse("memory://").unwrap();
        session_ctx.register_object_store(&url, Arc::new(store));

        let df = session_ctx
            .read_csv("memory:///", CsvReadOptions::new().terminator(Some(b'\r')))
            .await
            .unwrap();

        let result = df.collect().await.unwrap();

        let expected = [
            "+---+---+",
            "| a | b |",
            "+---+---+",
            "| 1 | 2 |",
            "| 3 | 4 |",
            "+---+---+",
        ];

        crate::assert_batches_eq!(expected, &result);

        let e = session_ctx
            .read_csv("memory:///", CsvReadOptions::new().terminator(Some(b'\n')))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err();
        assert_eq!(e.strip_backtrace(), "Arrow error: Csv error: incorrect number of fields for line 1, expected 2 got more than 2")
    }

    #[tokio::test]
    async fn test_create_external_table_with_terminator() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.sql(
            r#"
            CREATE EXTERNAL TABLE t1 (
            col1 TEXT,
            col2 TEXT
            ) STORED AS CSV
            LOCATION 'tests/data/cr_terminator.csv'
            OPTIONS ('format.terminator' E'\r', 'format.has_header' 'true');
    "#,
        )
        .await?
        .collect()
        .await?;

        let df = ctx.sql(r#"select * from t1"#).await?.collect().await?;
        let expected = [
            "+------+--------+",
            "| col1 | col2   |",
            "+------+--------+",
            "| id0  | value0 |",
            "| id1  | value1 |",
            "| id2  | value2 |",
            "| id3  | value3 |",
            "+------+--------+",
        ];
        crate::assert_batches_eq!(expected, &df);
        Ok(())
    }

    #[tokio::test]
    async fn test_create_external_table_with_terminator_with_newlines_in_values(
    ) -> Result<()> {
        let ctx = SessionContext::new();
        ctx.sql(r#"
            CREATE EXTERNAL TABLE t1 (
            col1 TEXT,
            col2 TEXT
            ) STORED AS CSV
            LOCATION 'tests/data/newlines_in_values_cr_terminator.csv'
            OPTIONS ('format.terminator' E'\r', 'format.has_header' 'true', 'format.newlines_in_values' 'true');
    "#).await?.collect().await?;

        let df = ctx.sql(r#"select * from t1"#).await?.collect().await?;
        let expected = [
            "+-------+-----------------------------+",
            "| col1  | col2                        |",
            "+-------+-----------------------------+",
            "| 1     | hello\rworld                 |",
            "| 2     | something\relse              |",
            "| 3     | \rmany\rlines\rmake\rgood test\r |",
            "| 4     | unquoted                    |",
            "| value | end                         |",
            "+-------+-----------------------------+",
        ];
        crate::assert_batches_eq!(expected, &df);
        Ok(())
    }

    #[tokio::test]
    async fn write_csv_results_error_handling() -> Result<()> {
        let ctx = SessionContext::new();

        // register a local file system object store
        let tmp_dir = TempDir::new()?;
        let local = Arc::new(LocalFileSystem::new_with_prefix(&tmp_dir)?);
        let local_url = Url::parse("file://local").unwrap();
        ctx.register_object_store(&local_url, local);
        let options = CsvReadOptions::default()
            .schema_infer_max_records(2)
            .has_header(true);
        let df = ctx.read_csv("tests/data/corrupt.csv", options).await?;

        let out_dir_url = "file://local/out";
        let e = df
            .write_csv(out_dir_url, DataFrameWriteOptions::new(), None)
            .await
            .expect_err("should fail because input file does not match inferred schema");
        assert_eq!(e.strip_backtrace(), "Arrow error: Parser error: Error while parsing value d for column 0 at line 4");
        Ok(())
    }

    #[tokio::test]
    async fn write_csv_results() -> Result<()> {
        // create partitioned input file and context
        let tmp_dir = TempDir::new()?;
        let ctx = SessionContext::new_with_config(
            SessionConfig::new()
                .with_target_partitions(8)
                .set_str("datafusion.catalog.has_header", "false"),
        );

        let schema = populate_csv_partitions(&tmp_dir, 8, ".csv")?;

        // register csv file with the execution context
        ctx.register_csv(
            "test",
            tmp_dir.path().to_str().unwrap(),
            CsvReadOptions::new().schema(&schema),
        )
        .await?;

        // register a local file system object store
        let tmp_dir = TempDir::new()?;
        let local = Arc::new(LocalFileSystem::new_with_prefix(&tmp_dir)?);
        let local_url = Url::parse("file://local").unwrap();

        ctx.register_object_store(&local_url, local);

        // execute a simple query and write the results to CSV
        let out_dir = tmp_dir.as_ref().to_str().unwrap().to_string() + "/out/";
        let out_dir_url = "file://local/out/";
        let df = ctx.sql("SELECT c1, c2 FROM test").await?;
        df.write_csv(out_dir_url, DataFrameWriteOptions::new(), None)
            .await?;

        // create a new context and verify that the results were saved to a partitioned csv file
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().set_str("datafusion.catalog.has_header", "false"),
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::UInt32, false),
            Field::new("c2", DataType::UInt64, false),
        ]));

        // get name of first part
        let paths = fs::read_dir(&out_dir).unwrap();
        let mut part_0_name: String = "".to_owned();
        for path in paths {
            let path = path.unwrap();
            let name = path
                .path()
                .file_name()
                .expect("Should be a file name")
                .to_str()
                .expect("Should be a str")
                .to_owned();
            if name.ends_with("_0.csv") {
                part_0_name = name;
                break;
            }
        }

        if part_0_name.is_empty() {
            panic!("Did not find part_0 in csv output files!")
        }
        // register each partition as well as the top level dir
        let csv_read_option = CsvReadOptions::new().schema(&schema).has_header(false);
        ctx.register_csv(
            "part0",
            &format!("{out_dir}/{part_0_name}"),
            csv_read_option.clone(),
        )
        .await?;
        ctx.register_csv("allparts", &out_dir, csv_read_option)
            .await?;

        let part0 = ctx.sql("SELECT c1, c2 FROM part0").await?.collect().await?;
        let allparts = ctx
            .sql("SELECT c1, c2 FROM allparts")
            .await?
            .collect()
            .await?;

        let allparts_count: usize = allparts.iter().map(|batch| batch.num_rows()).sum();

        assert_eq!(part0[0].schema(), allparts[0].schema());

        assert_eq!(allparts_count, 80);

        Ok(())
    }

    fn get_value(metrics: &MetricsSet, metric_name: &str) -> usize {
        match metrics.sum_by_name(metric_name) {
            Some(v) => v.as_usize(),
            _ => {
                panic!(
                    "Expected metric not found. Looking for '{metric_name}' in\n\n{metrics:#?}"
                );
            }
        }
    }

    /// Get the schema for the aggregate_test_* csv files with an additional filed not present in the files.
    fn aggr_test_schema_with_missing_col() -> SchemaRef {
        let fields =
            Fields::from_iter(aggr_test_schema().fields().iter().cloned().chain(
                std::iter::once(Arc::new(Field::new(
                    "missing_col",
                    DataType::Int64,
                    true,
                ))),
            ));

        let schema = Schema::new(fields);

        Arc::new(schema)
    }
}
