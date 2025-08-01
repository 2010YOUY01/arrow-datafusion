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

//! The table implementation.

use super::{
    helpers::{expr_applicable_for_cols, pruned_partition_list},
    ListingTableUrl, PartitionedFile,
};
use crate::{
    datasource::file_format::{file_compression_type::FileCompressionType, FileFormat},
    datasource::{create_ordering, physical_plan::FileSinkConfig},
    execution::context::SessionState,
};
use arrow::datatypes::{DataType, Field, SchemaBuilder, SchemaRef};
use arrow_schema::Schema;
use async_trait::async_trait;
use datafusion_catalog::{Session, TableProvider};
use datafusion_common::{
    config_datafusion_err, config_err, internal_err, plan_err, project_schema,
    stats::Precision, Constraints, DataFusionError, Result, SchemaExt,
};
use datafusion_datasource::{
    compute_all_files_statistics,
    file::FileSource,
    file_groups::FileGroup,
    file_scan_config::{FileScanConfig, FileScanConfigBuilder},
    schema_adapter::{DefaultSchemaAdapterFactory, SchemaAdapter, SchemaAdapterFactory},
};
use datafusion_execution::{
    cache::{cache_manager::FileStatisticsCache, cache_unit::DefaultFileStatisticsCache},
    config::SessionConfig,
};
use datafusion_expr::{
    dml::InsertOp, Expr, SortExpr, TableProviderFilterPushDown, TableType,
};
use datafusion_physical_expr::schema_rewriter::PhysicalExprAdapterFactory;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_plan::{empty::EmptyExec, ExecutionPlan, Statistics};
use futures::{future, stream, Stream, StreamExt, TryStreamExt};
use itertools::Itertools;
use object_store::ObjectStore;
use std::{any::Any, collections::HashMap, str::FromStr, sync::Arc};
/// Indicates the source of the schema for a [`ListingTable`]
// PartialEq required for assert_eq! in tests
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum SchemaSource {
    /// Schema is not yet set (initial state)
    #[default]
    Unset,
    /// Schema was inferred from first table_path
    Inferred,
    /// Schema was specified explicitly via with_schema
    Specified,
}

/// Configuration for creating a [`ListingTable`]
///
/// # Schema Evolution Support
///
/// This configuration supports schema evolution through the optional
/// [`SchemaAdapterFactory`]. You might want to override the default factory when you need:
///
/// - **Type coercion requirements**: When you need custom logic for converting between
///   different Arrow data types (e.g., Int32 ↔ Int64, Utf8 ↔ LargeUtf8)
/// - **Column mapping**: You need to map columns with a legacy name to a new name
/// - **Custom handling of missing columns**: By default they are filled in with nulls, but you may e.g. want to fill them in with `0` or `""`.
///
/// If not specified, a [`DefaultSchemaAdapterFactory`] will be used, which handles
/// basic schema compatibility cases.
///
#[derive(Debug, Clone, Default)]
pub struct ListingTableConfig {
    /// Paths on the `ObjectStore` for creating `ListingTable`.
    /// They should share the same schema and object store.
    pub table_paths: Vec<ListingTableUrl>,
    /// Optional `SchemaRef` for the to be created `ListingTable`.
    ///
    /// See details on [`ListingTableConfig::with_schema`]
    pub file_schema: Option<SchemaRef>,
    /// Optional [`ListingOptions`] for the to be created [`ListingTable`].
    ///
    /// See details on [`ListingTableConfig::with_listing_options`]
    pub options: Option<ListingOptions>,
    /// Tracks the source of the schema information
    schema_source: SchemaSource,
    /// Optional [`SchemaAdapterFactory`] for creating schema adapters
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    /// Optional [`PhysicalExprAdapterFactory`] for creating physical expression adapters
    physical_expr_adapter_factory: Option<Arc<dyn PhysicalExprAdapterFactory>>,
}

impl ListingTableConfig {
    /// Creates new [`ListingTableConfig`] for reading the specified URL
    pub fn new(table_path: ListingTableUrl) -> Self {
        Self {
            table_paths: vec![table_path],
            ..Default::default()
        }
    }

    /// Creates new [`ListingTableConfig`] with multiple table paths.
    ///
    /// See [`Self::infer_options`] for details on what happens with multiple paths
    pub fn new_with_multi_paths(table_paths: Vec<ListingTableUrl>) -> Self {
        Self {
            table_paths,
            ..Default::default()
        }
    }

    /// Returns the source of the schema for this configuration
    pub fn schema_source(&self) -> SchemaSource {
        self.schema_source
    }
    /// Set the `schema` for the overall [`ListingTable`]
    ///
    /// [`ListingTable`] will automatically coerce, when possible, the schema
    /// for individual files to match this schema.
    ///
    /// If a schema is not provided, it is inferred using
    /// [`Self::infer_schema`].
    ///
    /// If the schema is provided, it must contain only the fields in the file
    /// without the table partitioning columns.
    ///
    /// # Example: Specifying Table Schema
    /// ```rust
    /// # use std::sync::Arc;
    /// # use datafusion::datasource::listing::{ListingTableConfig, ListingOptions, ListingTableUrl};
    /// # use datafusion::datasource::file_format::parquet::ParquetFormat;
    /// # use arrow::datatypes::{Schema, Field, DataType};
    /// # let table_paths = ListingTableUrl::parse("file:///path/to/data").unwrap();
    /// # let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()));
    /// let schema = Arc::new(Schema::new(vec![
    ///     Field::new("id", DataType::Int64, false),
    ///     Field::new("name", DataType::Utf8, true),
    /// ]));
    ///
    /// let config = ListingTableConfig::new(table_paths)
    ///     .with_listing_options(listing_options)  // Set options first
    ///     .with_schema(schema);                    // Then set schema
    /// ```
    pub fn with_schema(self, schema: SchemaRef) -> Self {
        // Note: We preserve existing options state, but downstream code may expect
        // options to be set. Consider calling with_listing_options() or infer_options()
        // before operations that require options to be present.
        debug_assert!(
            self.options.is_some() || cfg!(test),
            "ListingTableConfig::with_schema called without options set. \
             Consider calling with_listing_options() or infer_options() first to avoid panics in downstream code."
        );

        Self {
            file_schema: Some(schema),
            schema_source: SchemaSource::Specified,
            ..self
        }
    }

    /// Add `listing_options` to [`ListingTableConfig`]
    ///
    /// If not provided, format and other options are inferred via
    /// [`Self::infer_options`].
    ///
    /// # Example: Configuring Parquet Files with Custom Options
    /// ```rust
    /// # use std::sync::Arc;
    /// # use datafusion::datasource::listing::{ListingTableConfig, ListingOptions, ListingTableUrl};
    /// # use datafusion::datasource::file_format::parquet::ParquetFormat;
    /// # let table_paths = ListingTableUrl::parse("file:///path/to/data").unwrap();
    /// let options = ListingOptions::new(Arc::new(ParquetFormat::default()))
    ///     .with_file_extension(".parquet")
    ///     .with_collect_stat(true);
    ///
    /// let config = ListingTableConfig::new(table_paths)
    ///     .with_listing_options(options);  // Configure file format and options
    /// ```
    pub fn with_listing_options(self, listing_options: ListingOptions) -> Self {
        // Note: This method properly sets options, but be aware that downstream
        // methods like infer_schema() and try_new() require both schema and options
        // to be set to function correctly.
        debug_assert!(
            !self.table_paths.is_empty() || cfg!(test),
            "ListingTableConfig::with_listing_options called without table_paths set. \
             Consider calling new() or new_with_multi_paths() first to establish table paths."
        );

        Self {
            options: Some(listing_options),
            ..self
        }
    }

    /// Returns a tuple of `(file_extension, optional compression_extension)`
    ///
    /// For example a path ending with blah.test.csv.gz returns `("csv", Some("gz"))`
    /// For example a path ending with blah.test.csv returns `("csv", None)`
    fn infer_file_extension_and_compression_type(
        path: &str,
    ) -> Result<(String, Option<String>)> {
        let mut exts = path.rsplit('.');

        let splitted = exts.next().unwrap_or("");

        let file_compression_type = FileCompressionType::from_str(splitted)
            .unwrap_or(FileCompressionType::UNCOMPRESSED);

        if file_compression_type.is_compressed() {
            let splitted2 = exts.next().unwrap_or("");
            Ok((splitted2.to_string(), Some(splitted.to_string())))
        } else {
            Ok((splitted.to_string(), None))
        }
    }

    /// Infer `ListingOptions` based on `table_path` and file suffix.
    ///
    /// The format is inferred based on the first `table_path`.
    pub async fn infer_options(self, state: &dyn Session) -> Result<Self> {
        let store = if let Some(url) = self.table_paths.first() {
            state.runtime_env().object_store(url)?
        } else {
            return Ok(self);
        };

        let file = self
            .table_paths
            .first()
            .unwrap()
            .list_all_files(state, store.as_ref(), "")
            .await?
            .next()
            .await
            .ok_or_else(|| DataFusionError::Internal("No files for table".into()))??;

        let (file_extension, maybe_compression_type) =
            ListingTableConfig::infer_file_extension_and_compression_type(
                file.location.as_ref(),
            )?;

        let mut format_options = HashMap::new();
        if let Some(ref compression_type) = maybe_compression_type {
            format_options
                .insert("format.compression".to_string(), compression_type.clone());
        }
        let state = state.as_any().downcast_ref::<SessionState>().unwrap();
        let file_format = state
            .get_file_format_factory(&file_extension)
            .ok_or(config_datafusion_err!(
                "No file_format found with extension {file_extension}"
            ))?
            .create(state, &format_options)?;

        let listing_file_extension =
            if let Some(compression_type) = maybe_compression_type {
                format!("{}.{}", &file_extension, &compression_type)
            } else {
                file_extension
            };

        let listing_options = ListingOptions::new(file_format)
            .with_file_extension(listing_file_extension)
            .with_target_partitions(state.config().target_partitions())
            .with_collect_stat(state.config().collect_statistics());

        Ok(Self {
            table_paths: self.table_paths,
            file_schema: self.file_schema,
            options: Some(listing_options),
            schema_source: self.schema_source,
            schema_adapter_factory: self.schema_adapter_factory,
            physical_expr_adapter_factory: self.physical_expr_adapter_factory,
        })
    }

    /// Infer the [`SchemaRef`] based on `table_path`s.
    ///
    /// This method infers the table schema using the first `table_path`.
    /// See [`ListingOptions::infer_schema`] for more details
    ///
    /// # Errors
    /// * if `self.options` is not set. See [`Self::with_listing_options`]
    pub async fn infer_schema(self, state: &dyn Session) -> Result<Self> {
        match self.options {
            Some(options) => {
                let ListingTableConfig {
                    table_paths,
                    file_schema,
                    options: _,
                    schema_source,
                    schema_adapter_factory,
                    physical_expr_adapter_factory,
                } = self;

                let (schema, new_schema_source) = match file_schema {
                    Some(schema) => (schema, schema_source), // Keep existing source if schema exists
                    None => {
                        if let Some(url) = table_paths.first() {
                            (
                                options.infer_schema(state, url).await?,
                                SchemaSource::Inferred,
                            )
                        } else {
                            (Arc::new(Schema::empty()), SchemaSource::Inferred)
                        }
                    }
                };

                Ok(Self {
                    table_paths,
                    file_schema: Some(schema),
                    options: Some(options),
                    schema_source: new_schema_source,
                    schema_adapter_factory,
                    physical_expr_adapter_factory,
                })
            }
            None => internal_err!("No `ListingOptions` set for inferring schema"),
        }
    }

    /// Convenience method to call both [`Self::infer_options`] and [`Self::infer_schema`]
    pub async fn infer(self, state: &dyn Session) -> Result<Self> {
        self.infer_options(state).await?.infer_schema(state).await
    }

    /// Infer the partition columns from `table_paths`.
    ///
    /// # Errors
    /// * if `self.options` is not set. See [`Self::with_listing_options`]
    pub async fn infer_partitions_from_path(self, state: &dyn Session) -> Result<Self> {
        match self.options {
            Some(options) => {
                let Some(url) = self.table_paths.first() else {
                    return config_err!("No table path found");
                };
                let partitions = options
                    .infer_partitions(state, url)
                    .await?
                    .into_iter()
                    .map(|col_name| {
                        (
                            col_name,
                            DataType::Dictionary(
                                Box::new(DataType::UInt16),
                                Box::new(DataType::Utf8),
                            ),
                        )
                    })
                    .collect::<Vec<_>>();
                let options = options.with_table_partition_cols(partitions);
                Ok(Self {
                    table_paths: self.table_paths,
                    file_schema: self.file_schema,
                    options: Some(options),
                    schema_source: self.schema_source,
                    schema_adapter_factory: self.schema_adapter_factory,
                    physical_expr_adapter_factory: self.physical_expr_adapter_factory,
                })
            }
            None => config_err!("No `ListingOptions` set for inferring schema"),
        }
    }

    /// Set the [`SchemaAdapterFactory`] for the [`ListingTable`]
    ///
    /// The schema adapter factory is used to create schema adapters that can
    /// handle schema evolution and type conversions when reading files with
    /// different schemas than the table schema.
    ///
    /// If not provided, a default schema adapter factory will be used.
    ///
    /// # Example: Custom Schema Adapter for Type Coercion
    /// ```rust
    /// # use std::sync::Arc;
    /// # use datafusion::datasource::listing::{ListingTableConfig, ListingOptions, ListingTableUrl};
    /// # use datafusion::datasource::schema_adapter::{SchemaAdapterFactory, SchemaAdapter};
    /// # use datafusion::datasource::file_format::parquet::ParquetFormat;
    /// # use arrow::datatypes::{SchemaRef, Schema, Field, DataType};
    /// #
    /// # #[derive(Debug)]
    /// # struct MySchemaAdapterFactory;
    /// # impl SchemaAdapterFactory for MySchemaAdapterFactory {
    /// #     fn create(&self, _projected_table_schema: SchemaRef, _file_schema: SchemaRef) -> Box<dyn SchemaAdapter> {
    /// #         unimplemented!()
    /// #     }
    /// # }
    /// # let table_paths = ListingTableUrl::parse("file:///path/to/data").unwrap();
    /// # let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()));
    /// # let table_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    /// let config = ListingTableConfig::new(table_paths)
    ///     .with_listing_options(listing_options)
    ///     .with_schema(table_schema)
    ///     .with_schema_adapter_factory(Arc::new(MySchemaAdapterFactory));
    /// ```
    pub fn with_schema_adapter_factory(
        self,
        schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Self {
        Self {
            schema_adapter_factory: Some(schema_adapter_factory),
            ..self
        }
    }

    /// Get the [`SchemaAdapterFactory`] for this configuration
    pub fn schema_adapter_factory(&self) -> Option<&Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.as_ref()
    }

    /// Set the [`PhysicalExprAdapterFactory`] for the [`ListingTable`]
    ///
    /// The expression adapter factory is used to create physical expression adapters that can
    /// handle schema evolution and type conversions when evaluating expressions
    /// with different schemas than the table schema.
    ///
    /// If not provided, a default physical expression adapter factory will be used unless a custom
    /// `SchemaAdapterFactory` is set, in which case only the `SchemaAdapterFactory` will be used.
    ///
    /// See <https://github.com/apache/datafusion/issues/16800> for details on this transition.
    pub fn with_physical_expr_adapter_factory(
        self,
        physical_expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory>,
    ) -> Self {
        Self {
            physical_expr_adapter_factory: Some(physical_expr_adapter_factory),
            ..self
        }
    }
}

/// Options for creating a [`ListingTable`]
#[derive(Clone, Debug)]
pub struct ListingOptions {
    /// A suffix on which files should be filtered (leave empty to
    /// keep all files on the path)
    pub file_extension: String,
    /// The file format
    pub format: Arc<dyn FileFormat>,
    /// The expected partition column names in the folder structure.
    /// See [Self::with_table_partition_cols] for details
    pub table_partition_cols: Vec<(String, DataType)>,
    /// Set true to try to guess statistics from the files.
    /// This can add a lot of overhead as it will usually require files
    /// to be opened and at least partially parsed.
    pub collect_stat: bool,
    /// Group files to avoid that the number of partitions exceeds
    /// this limit
    pub target_partitions: usize,
    /// Optional pre-known sort order(s). Must be `SortExpr`s.
    ///
    /// DataFusion may take advantage of this ordering to omit sorts
    /// or use more efficient algorithms. Currently sortedness must be
    /// provided if it is known by some external mechanism, but may in
    /// the future be automatically determined, for example using
    /// parquet metadata.
    ///
    /// See <https://github.com/apache/datafusion/issues/4177>
    ///
    /// NOTE: This attribute stores all equivalent orderings (the outer `Vec`)
    ///       where each ordering consists of an individual lexicographic
    ///       ordering (encapsulated by a `Vec<Expr>`). If there aren't
    ///       multiple equivalent orderings, the outer `Vec` will have a
    ///       single element.
    pub file_sort_order: Vec<Vec<SortExpr>>,
}

impl ListingOptions {
    /// Creates an options instance with the given format
    /// Default values:
    /// - use default file extension filter
    /// - no input partition to discover
    /// - one target partition
    /// - do not collect statistics
    pub fn new(format: Arc<dyn FileFormat>) -> Self {
        Self {
            file_extension: format.get_ext(),
            format,
            table_partition_cols: vec![],
            collect_stat: false,
            target_partitions: 1,
            file_sort_order: vec![],
        }
    }

    /// Set options from [`SessionConfig`] and returns self.
    ///
    /// Currently this sets `target_partitions` and `collect_stat`
    /// but if more options are added in the future that need to be coordinated
    /// they will be synchronized thorugh this method.
    pub fn with_session_config_options(mut self, config: &SessionConfig) -> Self {
        self = self.with_target_partitions(config.target_partitions());
        self = self.with_collect_stat(config.collect_statistics());
        self
    }

    /// Set file extension on [`ListingOptions`] and returns self.
    ///
    /// # Example
    /// ```
    /// # use std::sync::Arc;
    /// # use datafusion::prelude::SessionContext;
    /// # use datafusion::datasource::{listing::ListingOptions, file_format::parquet::ParquetFormat};
    ///
    /// let listing_options = ListingOptions::new(Arc::new(
    ///     ParquetFormat::default()
    ///   ))
    ///   .with_file_extension(".parquet");
    ///
    /// assert_eq!(listing_options.file_extension, ".parquet");
    /// ```
    pub fn with_file_extension(mut self, file_extension: impl Into<String>) -> Self {
        self.file_extension = file_extension.into();
        self
    }

    /// Optionally set file extension on [`ListingOptions`] and returns self.
    ///
    /// If `file_extension` is `None`, the file extension will not be changed
    ///
    /// # Example
    /// ```
    /// # use std::sync::Arc;
    /// # use datafusion::prelude::SessionContext;
    /// # use datafusion::datasource::{listing::ListingOptions, file_format::parquet::ParquetFormat};
    /// let extension = Some(".parquet");
    /// let listing_options = ListingOptions::new(Arc::new(
    ///     ParquetFormat::default()
    ///   ))
    ///   .with_file_extension_opt(extension);
    ///
    /// assert_eq!(listing_options.file_extension, ".parquet");
    /// ```
    pub fn with_file_extension_opt<S>(mut self, file_extension: Option<S>) -> Self
    where
        S: Into<String>,
    {
        if let Some(file_extension) = file_extension {
            self.file_extension = file_extension.into();
        }
        self
    }

    /// Set `table partition columns` on [`ListingOptions`] and returns self.
    ///
    /// "partition columns," used to support [Hive Partitioning], are
    /// columns added to the data that is read, based on the folder
    /// structure where the data resides.
    ///
    /// For example, give the following files in your filesystem:
    ///
    /// ```text
    /// /mnt/nyctaxi/year=2022/month=01/tripdata.parquet
    /// /mnt/nyctaxi/year=2021/month=12/tripdata.parquet
    /// /mnt/nyctaxi/year=2021/month=11/tripdata.parquet
    /// ```
    ///
    /// A [`ListingTable`] created at `/mnt/nyctaxi/` with partition
    /// columns "year" and "month" will include new `year` and `month`
    /// columns while reading the files. The `year` column would have
    /// value `2022` and the `month` column would have value `01` for
    /// the rows read from
    /// `/mnt/nyctaxi/year=2022/month=01/tripdata.parquet`
    ///
    ///# Notes
    ///
    /// - If only one level (e.g. `year` in the example above) is
    ///   specified, the other levels are ignored but the files are
    ///   still read.
    ///
    /// - Files that don't follow this partitioning scheme will be
    ///   ignored.
    ///
    /// - Since the columns have the same value for all rows read from
    ///   each individual file (such as dates), they are typically
    ///   dictionary encoded for efficiency. You may use
    ///   [`wrap_partition_type_in_dict`] to request a
    ///   dictionary-encoded type.
    ///
    /// - The partition columns are solely extracted from the file path. Especially they are NOT part of the parquet files itself.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow::datatypes::DataType;
    /// # use datafusion::prelude::col;
    /// # use datafusion::datasource::{listing::ListingOptions, file_format::parquet::ParquetFormat};
    ///
    /// // listing options for files with paths such as  `/mnt/data/col_a=x/col_b=y/data.parquet`
    /// // `col_a` and `col_b` will be included in the data read from those files
    /// let listing_options = ListingOptions::new(Arc::new(
    ///     ParquetFormat::default()
    ///   ))
    ///   .with_table_partition_cols(vec![("col_a".to_string(), DataType::Utf8),
    ///       ("col_b".to_string(), DataType::Utf8)]);
    ///
    /// assert_eq!(listing_options.table_partition_cols, vec![("col_a".to_string(), DataType::Utf8),
    ///     ("col_b".to_string(), DataType::Utf8)]);
    /// ```
    ///
    /// [Hive Partitioning]: https://docs.cloudera.com/HDPDocuments/HDP2/HDP-2.1.3/bk_system-admin-guide/content/hive_partitioned_tables.html
    /// [`wrap_partition_type_in_dict`]: crate::datasource::physical_plan::wrap_partition_type_in_dict
    pub fn with_table_partition_cols(
        mut self,
        table_partition_cols: Vec<(String, DataType)>,
    ) -> Self {
        self.table_partition_cols = table_partition_cols;
        self
    }

    /// Set stat collection on [`ListingOptions`] and returns self.
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use datafusion::datasource::{listing::ListingOptions, file_format::parquet::ParquetFormat};
    ///
    /// let listing_options = ListingOptions::new(Arc::new(
    ///     ParquetFormat::default()
    ///   ))
    ///   .with_collect_stat(true);
    ///
    /// assert_eq!(listing_options.collect_stat, true);
    /// ```
    pub fn with_collect_stat(mut self, collect_stat: bool) -> Self {
        self.collect_stat = collect_stat;
        self
    }

    /// Set number of target partitions on [`ListingOptions`] and returns self.
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use datafusion::datasource::{listing::ListingOptions, file_format::parquet::ParquetFormat};
    ///
    /// let listing_options = ListingOptions::new(Arc::new(
    ///     ParquetFormat::default()
    ///   ))
    ///   .with_target_partitions(8);
    ///
    /// assert_eq!(listing_options.target_partitions, 8);
    /// ```
    pub fn with_target_partitions(mut self, target_partitions: usize) -> Self {
        self.target_partitions = target_partitions;
        self
    }

    /// Set file sort order on [`ListingOptions`] and returns self.
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use datafusion::prelude::col;
    /// # use datafusion::datasource::{listing::ListingOptions, file_format::parquet::ParquetFormat};
    ///
    ///  // Tell datafusion that the files are sorted by column "a"
    ///  let file_sort_order = vec![vec![
    ///    col("a").sort(true, true)
    ///  ]];
    ///
    /// let listing_options = ListingOptions::new(Arc::new(
    ///     ParquetFormat::default()
    ///   ))
    ///   .with_file_sort_order(file_sort_order.clone());
    ///
    /// assert_eq!(listing_options.file_sort_order, file_sort_order);
    /// ```
    pub fn with_file_sort_order(mut self, file_sort_order: Vec<Vec<SortExpr>>) -> Self {
        self.file_sort_order = file_sort_order;
        self
    }

    /// Infer the schema of the files at the given path on the provided object store.
    ///
    /// If the table_path contains one or more files (i.e. it is a directory /
    /// prefix of files) their schema is merged by calling [`FileFormat::infer_schema`]
    ///
    /// Note: The inferred schema does not include any partitioning columns.
    ///
    /// This method is called as part of creating a [`ListingTable`].
    pub async fn infer_schema<'a>(
        &'a self,
        state: &dyn Session,
        table_path: &'a ListingTableUrl,
    ) -> Result<SchemaRef> {
        let store = state.runtime_env().object_store(table_path)?;

        let files: Vec<_> = table_path
            .list_all_files(state, store.as_ref(), &self.file_extension)
            .await?
            // Empty files cannot affect schema but may throw when trying to read for it
            .try_filter(|object_meta| future::ready(object_meta.size > 0))
            .try_collect()
            .await?;

        let schema = self.format.infer_schema(state, &store, &files).await?;

        Ok(schema)
    }

    /// Infers the partition columns stored in `LOCATION` and compares
    /// them with the columns provided in `PARTITIONED BY` to help prevent
    /// accidental corrupts of partitioned tables.
    ///
    /// Allows specifying partial partitions.
    pub async fn validate_partitions(
        &self,
        state: &dyn Session,
        table_path: &ListingTableUrl,
    ) -> Result<()> {
        if self.table_partition_cols.is_empty() {
            return Ok(());
        }

        if !table_path.is_collection() {
            return plan_err!(
                "Can't create a partitioned table backed by a single file, \
                perhaps the URL is missing a trailing slash?"
            );
        }

        let inferred = self.infer_partitions(state, table_path).await?;

        // no partitioned files found on disk
        if inferred.is_empty() {
            return Ok(());
        }

        let table_partition_names = self
            .table_partition_cols
            .iter()
            .map(|(col_name, _)| col_name.clone())
            .collect_vec();

        if inferred.len() < table_partition_names.len() {
            return plan_err!(
                "Inferred partitions to be {:?}, but got {:?}",
                inferred,
                table_partition_names
            );
        }

        // match prefix to allow creating tables with partial partitions
        for (idx, col) in table_partition_names.iter().enumerate() {
            if &inferred[idx] != col {
                return plan_err!(
                    "Inferred partitions to be {:?}, but got {:?}",
                    inferred,
                    table_partition_names
                );
            }
        }

        Ok(())
    }

    /// Infer the partitioning at the given path on the provided object store.
    /// For performance reasons, it doesn't read all the files on disk
    /// and therefore may fail to detect invalid partitioning.
    pub(crate) async fn infer_partitions(
        &self,
        state: &dyn Session,
        table_path: &ListingTableUrl,
    ) -> Result<Vec<String>> {
        let store = state.runtime_env().object_store(table_path)?;

        // only use 10 files for inference
        // This can fail to detect inconsistent partition keys
        // A DFS traversal approach of the store can help here
        let files: Vec<_> = table_path
            .list_all_files(state, store.as_ref(), &self.file_extension)
            .await?
            .take(10)
            .try_collect()
            .await?;

        let stripped_path_parts = files.iter().map(|file| {
            table_path
                .strip_prefix(&file.location)
                .unwrap()
                .collect_vec()
        });

        let partition_keys = stripped_path_parts
            .map(|path_parts| {
                path_parts
                    .into_iter()
                    .rev()
                    .skip(1) // get parents only; skip the file itself
                    .rev()
                    .map(|s| s.split('=').take(1).collect())
                    .collect_vec()
            })
            .collect_vec();

        match partition_keys.into_iter().all_equal_value() {
            Ok(v) => Ok(v),
            Err(None) => Ok(vec![]),
            Err(Some(diff)) => {
                let mut sorted_diff = [diff.0, diff.1];
                sorted_diff.sort();
                plan_err!("Found mixed partition values on disk {:?}", sorted_diff)
            }
        }
    }
}

/// Reads data from one or more files as a single table.
///
/// Implements [`TableProvider`], a DataFusion data source. The files are read
/// using an  [`ObjectStore`] instance, for example from local files or objects
/// from AWS S3.
///
/// # Reading Directories
/// For example, given the `table1` directory (or object store prefix)
///
/// ```text
/// table1
///  ├── file1.parquet
///  └── file2.parquet
/// ```
///
/// A `ListingTable` would read the files `file1.parquet` and `file2.parquet` as
/// a single table, merging the schemas if the files have compatible but not
/// identical schemas.
///
/// Given the `table2` directory (or object store prefix)
///
/// ```text
/// table2
///  ├── date=2024-06-01
///  │    ├── file3.parquet
///  │    └── file4.parquet
///  └── date=2024-06-02
///       └── file5.parquet
/// ```
///
/// A `ListingTable` would read the files `file3.parquet`, `file4.parquet`, and
/// `file5.parquet` as a single table, again merging schemas if necessary.
///
/// Given the hive style partitioning structure (e.g,. directories named
/// `date=2024-06-01` and `date=2026-06-02`), `ListingTable` also adds a `date`
/// column when reading the table:
/// * The files in `table2/date=2024-06-01` will have the value `2024-06-01`
/// * The files in `table2/date=2024-06-02` will have the value `2024-06-02`.
///
/// If the query has a predicate like `WHERE date = '2024-06-01'`
/// only the corresponding directory will be read.
///
/// `ListingTable` also supports limit, filter and projection pushdown for formats that
/// support it as such as Parquet.
///
/// # See Also
///
/// 1. [`ListingTableConfig`]: Configuration options
/// 1. [`DataSourceExec`]: `ExecutionPlan` used by `ListingTable`
///
/// [`DataSourceExec`]: crate::datasource::source::DataSourceExec
///
/// # Example: Read a directory of parquet files using a [`ListingTable`]
///
/// ```no_run
/// # use datafusion::prelude::SessionContext;
/// # use datafusion::error::Result;
/// # use std::sync::Arc;
/// # use datafusion::datasource::{
/// #   listing::{
/// #      ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
/// #   },
/// #   file_format::parquet::ParquetFormat,
/// # };
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let ctx = SessionContext::new();
/// let session_state = ctx.state();
/// let table_path = "/path/to/parquet";
///
/// // Parse the path
/// let table_path = ListingTableUrl::parse(table_path)?;
///
/// // Create default parquet options
/// let file_format = ParquetFormat::new();
/// let listing_options = ListingOptions::new(Arc::new(file_format))
///   .with_file_extension(".parquet");
///
/// // Resolve the schema
/// let resolved_schema = listing_options
///    .infer_schema(&session_state, &table_path)
///    .await?;
///
/// let config = ListingTableConfig::new(table_path)
///   .with_listing_options(listing_options)
///   .with_schema(resolved_schema);
///
/// // Create a new TableProvider
/// let provider = Arc::new(ListingTable::try_new(config)?);
///
/// // This provider can now be read as a dataframe:
/// let df = ctx.read_table(provider.clone());
///
/// // or registered as a named table:
/// ctx.register_table("my_table", provider);
///
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ListingTable {
    table_paths: Vec<ListingTableUrl>,
    /// `file_schema` contains only the columns physically stored in the data files themselves.
    ///     - Represents the actual fields found in files like Parquet, CSV, etc.
    ///     - Used when reading the raw data from files
    file_schema: SchemaRef,
    /// `table_schema` combines `file_schema` + partition columns
    ///     - Partition columns are derived from directory paths (not stored in files)
    ///     - These are columns like "year=2022/month=01" in paths like `/data/year=2022/month=01/file.parquet`
    table_schema: SchemaRef,
    /// Indicates how the schema was derived (inferred or explicitly specified)
    schema_source: SchemaSource,
    options: ListingOptions,
    definition: Option<String>,
    collected_statistics: FileStatisticsCache,
    constraints: Constraints,
    column_defaults: HashMap<String, Expr>,
    /// Optional [`SchemaAdapterFactory`] for creating schema adapters
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    /// Optional [`PhysicalExprAdapterFactory`] for creating physical expression adapters
    expr_adapter_factory: Option<Arc<dyn PhysicalExprAdapterFactory>>,
}

impl ListingTable {
    /// Create new [`ListingTable`]
    ///
    /// See documentation and example on [`ListingTable`] and [`ListingTableConfig`]
    pub fn try_new(config: ListingTableConfig) -> Result<Self> {
        // Extract schema_source before moving other parts of the config
        let schema_source = config.schema_source();

        let file_schema = config
            .file_schema
            .ok_or_else(|| DataFusionError::Internal("No schema provided.".into()))?;

        let options = config.options.ok_or_else(|| {
            DataFusionError::Internal("No ListingOptions provided".into())
        })?;

        // Add the partition columns to the file schema
        let mut builder = SchemaBuilder::from(file_schema.as_ref().to_owned());
        for (part_col_name, part_col_type) in &options.table_partition_cols {
            builder.push(Field::new(part_col_name, part_col_type.clone(), false));
        }

        let table_schema = Arc::new(
            builder
                .finish()
                .with_metadata(file_schema.metadata().clone()),
        );

        let table = Self {
            table_paths: config.table_paths,
            file_schema,
            table_schema,
            schema_source,
            options,
            definition: None,
            collected_statistics: Arc::new(DefaultFileStatisticsCache::default()),
            constraints: Constraints::default(),
            column_defaults: HashMap::new(),
            schema_adapter_factory: config.schema_adapter_factory,
            expr_adapter_factory: config.physical_expr_adapter_factory,
        };

        Ok(table)
    }

    /// Assign constraints
    pub fn with_constraints(mut self, constraints: Constraints) -> Self {
        self.constraints = constraints;
        self
    }

    /// Assign column defaults
    pub fn with_column_defaults(
        mut self,
        column_defaults: HashMap<String, Expr>,
    ) -> Self {
        self.column_defaults = column_defaults;
        self
    }

    /// Set the [`FileStatisticsCache`] used to cache parquet file statistics.
    ///
    /// Setting a statistics cache on the `SessionContext` can avoid refetching statistics
    /// multiple times in the same session.
    ///
    /// If `None`, creates a new [`DefaultFileStatisticsCache`] scoped to this query.
    pub fn with_cache(mut self, cache: Option<FileStatisticsCache>) -> Self {
        self.collected_statistics =
            cache.unwrap_or_else(|| Arc::new(DefaultFileStatisticsCache::default()));
        self
    }

    /// Specify the SQL definition for this table, if any
    pub fn with_definition(mut self, definition: Option<String>) -> Self {
        self.definition = definition;
        self
    }

    /// Get paths ref
    pub fn table_paths(&self) -> &Vec<ListingTableUrl> {
        &self.table_paths
    }

    /// Get options ref
    pub fn options(&self) -> &ListingOptions {
        &self.options
    }

    /// Get the schema source
    pub fn schema_source(&self) -> SchemaSource {
        self.schema_source
    }

    /// Set the [`SchemaAdapterFactory`] for this [`ListingTable`]
    ///
    /// The schema adapter factory is used to create schema adapters that can
    /// handle schema evolution and type conversions when reading files with
    /// different schemas than the table schema.
    ///
    /// # Example: Adding Schema Evolution Support
    /// ```rust
    /// # use std::sync::Arc;
    /// # use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingOptions, ListingTableUrl};
    /// # use datafusion::datasource::schema_adapter::{DefaultSchemaAdapterFactory, SchemaAdapter};
    /// # use datafusion::datasource::file_format::parquet::ParquetFormat;
    /// # use arrow::datatypes::{SchemaRef, Schema, Field, DataType};
    /// # let table_path = ListingTableUrl::parse("file:///path/to/data").unwrap();
    /// # let options = ListingOptions::new(Arc::new(ParquetFormat::default()));
    /// # let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    /// # let config = ListingTableConfig::new(table_path).with_listing_options(options).with_schema(schema);
    /// # let table = ListingTable::try_new(config).unwrap();
    /// let table_with_evolution = table
    ///     .with_schema_adapter_factory(Arc::new(DefaultSchemaAdapterFactory));
    /// ```
    /// See [`ListingTableConfig::with_schema_adapter_factory`] for an example of custom SchemaAdapterFactory.
    pub fn with_schema_adapter_factory(
        self,
        schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Self {
        Self {
            schema_adapter_factory: Some(schema_adapter_factory),
            ..self
        }
    }

    /// Get the [`SchemaAdapterFactory`] for this table
    pub fn schema_adapter_factory(&self) -> Option<&Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.as_ref()
    }

    /// Creates a schema adapter for mapping between file and table schemas
    ///
    /// Uses the configured schema adapter factory if available, otherwise falls back
    /// to the default implementation.
    fn create_schema_adapter(&self) -> Box<dyn SchemaAdapter> {
        let table_schema = self.schema();
        match &self.schema_adapter_factory {
            Some(factory) => {
                factory.create_with_projected_schema(Arc::clone(&table_schema))
            }
            None => DefaultSchemaAdapterFactory::from_schema(Arc::clone(&table_schema)),
        }
    }

    /// Creates a file source and applies schema adapter factory if available
    fn create_file_source_with_schema_adapter(&self) -> Result<Arc<dyn FileSource>> {
        let mut source = self.options.format.file_source();
        // Apply schema adapter to source if available
        //
        // The source will use this SchemaAdapter to adapt data batches as they flow up the plan.
        // Note: ListingTable also creates a SchemaAdapter in `scan()` but that is only used to adapt collected statistics.
        if let Some(factory) = &self.schema_adapter_factory {
            source = source.with_schema_adapter_factory(Arc::clone(factory))?;
        }
        Ok(source)
    }

    /// If file_sort_order is specified, creates the appropriate physical expressions
    fn try_create_output_ordering(&self) -> Result<Vec<LexOrdering>> {
        create_ordering(&self.table_schema, &self.options.file_sort_order)
    }
}

// Expressions can be used for parttion pruning if they can be evaluated using
// only the partiton columns and there are partition columns.
fn can_be_evaluted_for_partition_pruning(
    partition_column_names: &[&str],
    expr: &Expr,
) -> bool {
    !partition_column_names.is_empty()
        && expr_applicable_for_cols(partition_column_names, expr)
}

#[async_trait]
impl TableProvider for ListingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.constraints)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // extract types of partition columns
        let table_partition_cols = self
            .options
            .table_partition_cols
            .iter()
            .map(|col| Ok(self.table_schema.field_with_name(&col.0)?.clone()))
            .collect::<Result<Vec<_>>>()?;

        let table_partition_col_names = table_partition_cols
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        // If the filters can be resolved using only partition cols, there is no need to
        // pushdown it to TableScan, otherwise, `unhandled` pruning predicates will be generated
        let (partition_filters, filters): (Vec<_>, Vec<_>) =
            filters.iter().cloned().partition(|filter| {
                can_be_evaluted_for_partition_pruning(&table_partition_col_names, filter)
            });

        // We should not limit the number of partitioned files to scan if there are filters and limit
        // at the same time. This is because the limit should be applied after the filters are applied.
        let statistic_file_limit = if filters.is_empty() { limit } else { None };

        let (mut partitioned_file_lists, statistics) = self
            .list_files_for_scan(state, &partition_filters, statistic_file_limit)
            .await?;

        // if no files need to be read, return an `EmptyExec`
        if partitioned_file_lists.is_empty() {
            let projected_schema = project_schema(&self.schema(), projection)?;
            return Ok(Arc::new(EmptyExec::new(projected_schema)));
        }

        let output_ordering = self.try_create_output_ordering()?;
        match state
            .config_options()
            .execution
            .split_file_groups_by_statistics
            .then(|| {
                output_ordering.first().map(|output_ordering| {
                    FileScanConfig::split_groups_by_statistics_with_target_partitions(
                        &self.table_schema,
                        &partitioned_file_lists,
                        output_ordering,
                        self.options.target_partitions,
                    )
                })
            })
            .flatten()
        {
            Some(Err(e)) => log::debug!("failed to split file groups by statistics: {e}"),
            Some(Ok(new_groups)) => {
                if new_groups.len() <= self.options.target_partitions {
                    partitioned_file_lists = new_groups;
                } else {
                    log::debug!("attempted to split file groups by statistics, but there were more file groups than target_partitions; falling back to unordered")
                }
            }
            None => {} // no ordering required
        };

        let Some(object_store_url) =
            self.table_paths.first().map(ListingTableUrl::object_store)
        else {
            return Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))));
        };

        let file_source = self.create_file_source_with_schema_adapter()?;

        // create the execution plan
        self.options
            .format
            .create_physical_plan(
                state,
                FileScanConfigBuilder::new(
                    object_store_url,
                    Arc::clone(&self.file_schema),
                    file_source,
                )
                .with_file_groups(partitioned_file_lists)
                .with_constraints(self.constraints.clone())
                .with_statistics(statistics)
                .with_projection(projection.cloned())
                .with_limit(limit)
                .with_output_ordering(output_ordering)
                .with_table_partition_cols(table_partition_cols)
                .with_expr_adapter(self.expr_adapter_factory.clone())
                .build(),
            )
            .await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let partition_column_names = self
            .options
            .table_partition_cols
            .iter()
            .map(|col| col.0.as_str())
            .collect::<Vec<_>>();
        filters
            .iter()
            .map(|filter| {
                if can_be_evaluted_for_partition_pruning(&partition_column_names, filter)
                {
                    // if filter can be handled by partition pruning, it is exact
                    return Ok(TableProviderFilterPushDown::Exact);
                }

                Ok(TableProviderFilterPushDown::Inexact)
            })
            .collect()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.definition.as_deref()
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Check that the schema of the plan matches the schema of this table.
        self.schema()
            .logically_equivalent_names_and_types(&input.schema())?;

        let table_path = &self.table_paths()[0];
        if !table_path.is_collection() {
            return plan_err!(
                "Inserting into a ListingTable backed by a single file is not supported, URL is possibly missing a trailing `/`. \
                To append to an existing file use StreamTable, e.g. by using CREATE UNBOUNDED EXTERNAL TABLE"
            );
        }

        // Get the object store for the table path.
        let store = state.runtime_env().object_store(table_path)?;

        let file_list_stream = pruned_partition_list(
            state,
            store.as_ref(),
            table_path,
            &[],
            &self.options.file_extension,
            &self.options.table_partition_cols,
        )
        .await?;

        let file_group = file_list_stream.try_collect::<Vec<_>>().await?.into();
        let keep_partition_by_columns =
            state.config_options().execution.keep_partition_by_columns;

        // Sink related option, apart from format
        let config = FileSinkConfig {
            original_url: String::default(),
            object_store_url: self.table_paths()[0].object_store(),
            table_paths: self.table_paths().clone(),
            file_group,
            output_schema: self.schema(),
            table_partition_cols: self.options.table_partition_cols.clone(),
            insert_op,
            keep_partition_by_columns,
            file_extension: self.options().format.get_ext(),
        };

        let orderings = self.try_create_output_ordering()?;
        // It is sufficient to pass only one of the equivalent orderings:
        let order_requirements = orderings.into_iter().next().map(Into::into);

        self.options()
            .format
            .create_writer_physical_plan(input, state, config, order_requirements)
            .await
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.column_defaults.get(column)
    }
}

impl ListingTable {
    /// Get the list of files for a scan as well as the file level statistics.
    /// The list is grouped to let the execution plan know how the files should
    /// be distributed to different threads / executors.
    async fn list_files_for_scan<'a>(
        &'a self,
        ctx: &'a dyn Session,
        filters: &'a [Expr],
        limit: Option<usize>,
    ) -> Result<(Vec<FileGroup>, Statistics)> {
        let store = if let Some(url) = self.table_paths.first() {
            ctx.runtime_env().object_store(url)?
        } else {
            return Ok((vec![], Statistics::new_unknown(&self.file_schema)));
        };
        // list files (with partitions)
        let file_list = future::try_join_all(self.table_paths.iter().map(|table_path| {
            pruned_partition_list(
                ctx,
                store.as_ref(),
                table_path,
                filters,
                &self.options.file_extension,
                &self.options.table_partition_cols,
            )
        }))
        .await?;
        let meta_fetch_concurrency =
            ctx.config_options().execution.meta_fetch_concurrency;
        let file_list = stream::iter(file_list).flatten_unordered(meta_fetch_concurrency);
        // collect the statistics if required by the config
        let files = file_list
            .map(|part_file| async {
                let part_file = part_file?;
                let statistics = if self.options.collect_stat {
                    self.do_collect_statistics(ctx, &store, &part_file).await?
                } else {
                    Arc::new(Statistics::new_unknown(&self.file_schema))
                };
                Ok(part_file.with_statistics(statistics))
            })
            .boxed()
            .buffer_unordered(ctx.config_options().execution.meta_fetch_concurrency);

        let (file_group, inexact_stats) =
            get_files_with_limit(files, limit, self.options.collect_stat).await?;

        let file_groups = file_group.split_files(self.options.target_partitions);
        let (mut file_groups, mut stats) = compute_all_files_statistics(
            file_groups,
            self.schema(),
            self.options.collect_stat,
            inexact_stats,
        )?;

        let schema_adapter = self.create_schema_adapter();
        let (schema_mapper, _) = schema_adapter.map_schema(self.file_schema.as_ref())?;

        stats.column_statistics =
            schema_mapper.map_column_statistics(&stats.column_statistics)?;
        file_groups.iter_mut().try_for_each(|file_group| {
            if let Some(stat) = file_group.statistics_mut() {
                stat.column_statistics =
                    schema_mapper.map_column_statistics(&stat.column_statistics)?;
            }
            Ok::<_, DataFusionError>(())
        })?;
        Ok((file_groups, stats))
    }

    /// Collects statistics for a given partitioned file.
    ///
    /// This method first checks if the statistics for the given file are already cached.
    /// If they are, it returns the cached statistics.
    /// If they are not, it infers the statistics from the file and stores them in the cache.
    async fn do_collect_statistics(
        &self,
        ctx: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        part_file: &PartitionedFile,
    ) -> Result<Arc<Statistics>> {
        match self
            .collected_statistics
            .get_with_extra(&part_file.object_meta.location, &part_file.object_meta)
        {
            Some(statistics) => Ok(statistics),
            None => {
                let statistics = self
                    .options
                    .format
                    .infer_stats(
                        ctx,
                        store,
                        Arc::clone(&self.file_schema),
                        &part_file.object_meta,
                    )
                    .await?;
                let statistics = Arc::new(statistics);
                self.collected_statistics.put_with_extra(
                    &part_file.object_meta.location,
                    Arc::clone(&statistics),
                    &part_file.object_meta,
                );
                Ok(statistics)
            }
        }
    }
}

/// Processes a stream of partitioned files and returns a `FileGroup` containing the files.
///
/// This function collects files from the provided stream until either:
/// 1. The stream is exhausted
/// 2. The accumulated number of rows exceeds the provided `limit` (if specified)
///
/// # Arguments
/// * `files` - A stream of `Result<PartitionedFile>` items to process
/// * `limit` - An optional row count limit. If provided, the function will stop collecting files
///   once the accumulated number of rows exceeds this limit
/// * `collect_stats` - Whether to collect and accumulate statistics from the files
///
/// # Returns
/// A `Result` containing a `FileGroup` with the collected files
/// and a boolean indicating whether the statistics are inexact.
///
/// # Note
/// The function will continue processing files if statistics are not available or if the
/// limit is not provided. If `collect_stats` is false, statistics won't be accumulated
/// but files will still be collected.
async fn get_files_with_limit(
    files: impl Stream<Item = Result<PartitionedFile>>,
    limit: Option<usize>,
    collect_stats: bool,
) -> Result<(FileGroup, bool)> {
    let mut file_group = FileGroup::default();
    // Fusing the stream allows us to call next safely even once it is finished.
    let mut all_files = Box::pin(files.fuse());
    enum ProcessingState {
        ReadingFiles,
        ReachedLimit,
    }

    let mut state = ProcessingState::ReadingFiles;
    let mut num_rows = Precision::Absent;

    while let Some(file_result) = all_files.next().await {
        // Early exit if we've already reached our limit
        if matches!(state, ProcessingState::ReachedLimit) {
            break;
        }

        let file = file_result?;

        // Update file statistics regardless of state
        if collect_stats {
            if let Some(file_stats) = &file.statistics {
                num_rows = if file_group.is_empty() {
                    // For the first file, just take its row count
                    file_stats.num_rows
                } else {
                    // For subsequent files, accumulate the counts
                    num_rows.add(&file_stats.num_rows)
                };
            }
        }

        // Always add the file to our group
        file_group.push(file);

        // Check if we've hit the limit (if one was specified)
        if let Some(limit) = limit {
            if let Precision::Exact(row_count) = num_rows {
                if row_count > limit {
                    state = ProcessingState::ReachedLimit;
                }
            }
        }
    }
    // If we still have files in the stream, it means that the limit kicked
    // in, and the statistic could have been different had we processed the
    // files in a different order.
    let inexact_stats = all_files.next().await.is_some();
    Ok((file_group, inexact_stats))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "parquet")]
    use crate::datasource::file_format::parquet::ParquetFormat;
    use crate::prelude::*;
    use crate::{
        datasource::{
            file_format::csv::CsvFormat, file_format::json::JsonFormat,
            provider_as_source, DefaultTableSource, MemTable,
        },
        execution::options::ArrowReadOptions,
        test::{
            columns, object_store::ensure_head_concurrency,
            object_store::make_test_store_and_state, object_store::register_test_store,
        },
    };
    use arrow::{compute::SortOptions, record_batch::RecordBatch};
    use datafusion_common::{
        assert_contains,
        stats::Precision,
        test_util::{batches_to_string, datafusion_test_data},
        ColumnStatistics, ScalarValue,
    };
    use datafusion_datasource::schema_adapter::{
        SchemaAdapter, SchemaAdapterFactory, SchemaMapper,
    };
    use datafusion_expr::{BinaryExpr, LogicalPlanBuilder, Operator};
    use datafusion_physical_expr::PhysicalSortExpr;
    use datafusion_physical_plan::{collect, ExecutionPlanProperties};
    use rstest::rstest;
    use std::io::Write;
    use tempfile::TempDir;
    use url::Url;

    const DUMMY_NULL_COUNT: Precision<usize> = Precision::Exact(42);

    /// Creates a test schema with standard field types used in tests
    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Float32, true),
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::Boolean, true),
            Field::new("c4", DataType::Utf8, true),
        ]))
    }

    /// Helper function to generate test file paths with given prefix, count, and optional start index
    fn generate_test_files(prefix: &str, count: usize) -> Vec<String> {
        generate_test_files_with_start(prefix, count, 0)
    }

    /// Helper function to generate test file paths with given prefix, count, and start index
    fn generate_test_files_with_start(
        prefix: &str,
        count: usize,
        start_index: usize,
    ) -> Vec<String> {
        (start_index..start_index + count)
            .map(|i| format!("{prefix}/file{i}"))
            .collect()
    }

    #[tokio::test]
    async fn test_schema_source_tracking_comprehensive() -> Result<()> {
        let ctx = SessionContext::new();
        let testdata = datafusion_test_data();
        let filename = format!("{testdata}/aggregate_simple.csv");
        let table_path = ListingTableUrl::parse(filename).unwrap();

        // Test default schema source
        let config = ListingTableConfig::new(table_path.clone());
        assert_eq!(config.schema_source(), SchemaSource::Unset);

        // Test schema source after setting a schema explicitly
        let provided_schema = create_test_schema();
        let config_with_schema = config.clone().with_schema(provided_schema.clone());
        assert_eq!(config_with_schema.schema_source(), SchemaSource::Specified);

        // Test schema source after inferring schema
        let format = CsvFormat::default();
        let options = ListingOptions::new(Arc::new(format));
        let config_with_options = config.with_listing_options(options.clone());
        assert_eq!(config_with_options.schema_source(), SchemaSource::Unset);

        let config_with_inferred = config_with_options.infer_schema(&ctx.state()).await?;
        assert_eq!(config_with_inferred.schema_source(), SchemaSource::Inferred);

        // Test schema preservation through operations
        let config_with_schema_and_options = config_with_schema
            .clone()
            .with_listing_options(options.clone());
        assert_eq!(
            config_with_schema_and_options.schema_source(),
            SchemaSource::Specified
        );

        // Make sure inferred schema doesn't override specified schema
        let config_with_schema_and_infer = config_with_schema_and_options
            .clone()
            .infer(&ctx.state())
            .await?;
        assert_eq!(
            config_with_schema_and_infer.schema_source(),
            SchemaSource::Specified
        );

        // Verify sources in actual ListingTable objects
        let table_specified = ListingTable::try_new(config_with_schema_and_options)?;
        assert_eq!(table_specified.schema_source(), SchemaSource::Specified);

        let table_inferred = ListingTable::try_new(config_with_inferred)?;
        assert_eq!(table_inferred.schema_source(), SchemaSource::Inferred);

        Ok(())
    }

    #[tokio::test]
    async fn read_single_file() -> Result<()> {
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_collect_statistics(true),
        );

        let table = load_table(&ctx, "alltypes_plain.parquet").await?;
        let projection = None;
        let exec = table
            .scan(&ctx.state(), projection, &[], None)
            .await
            .expect("Scan table");

        assert_eq!(exec.children().len(), 0);
        assert_eq!(exec.output_partitioning().partition_count(), 1);

        // test metadata
        assert_eq!(
            exec.partition_statistics(None)?.num_rows,
            Precision::Exact(8)
        );
        assert_eq!(
            exec.partition_statistics(None)?.total_byte_size,
            Precision::Exact(671)
        );

        Ok(())
    }

    #[cfg(feature = "parquet")]
    #[tokio::test]
    async fn test_try_create_output_ordering() {
        let testdata = crate::test_util::parquet_test_data();
        let filename = format!("{}/{}", testdata, "alltypes_plain.parquet");
        let table_path = ListingTableUrl::parse(filename).unwrap();

        let ctx = SessionContext::new();
        let state = ctx.state();
        let options = ListingOptions::new(Arc::new(ParquetFormat::default()));
        let schema = options.infer_schema(&state, &table_path).await.unwrap();

        use crate::datasource::file_format::parquet::ParquetFormat;
        use datafusion_physical_plan::expressions::col as physical_col;
        use std::ops::Add;

        // (file_sort_order, expected_result)
        let cases = vec![
            (vec![], Ok(Vec::<LexOrdering>::new())),
            // sort expr, but non column
            (
                vec![vec![
                    col("int_col").add(lit(1)).sort(true, true),
                ]],
                Err("Expected single column reference in sort_order[0][0], got int_col + Int32(1)"),
            ),
            // ok with one column
            (
                vec![vec![col("string_col").sort(true, false)]],
                Ok(vec![[PhysicalSortExpr {
                            expr: physical_col("string_col", &schema).unwrap(),
                            options: SortOptions {
                                descending: false,
                                nulls_first: false,
                            },
                        }].into(),
                ])
            ),
            // ok with two columns, different options
            (
                vec![vec![
                    col("string_col").sort(true, false),
                    col("int_col").sort(false, true),
                ]],
                Ok(vec![[
                            PhysicalSortExpr::new_default(physical_col("string_col", &schema).unwrap())
                                        .asc()
                                        .nulls_last(),
                            PhysicalSortExpr::new_default(physical_col("int_col", &schema).unwrap())
                                        .desc()
                                        .nulls_first()
                        ].into(),
                ])
            ),
        ];

        for (file_sort_order, expected_result) in cases {
            let options = options.clone().with_file_sort_order(file_sort_order);

            let config = ListingTableConfig::new(table_path.clone())
                .with_listing_options(options)
                .with_schema(schema.clone());

            let table =
                ListingTable::try_new(config.clone()).expect("Creating the table");
            let ordering_result = table.try_create_output_ordering();

            match (expected_result, ordering_result) {
                (Ok(expected), Ok(result)) => {
                    assert_eq!(expected, result);
                }
                (Err(expected), Err(result)) => {
                    // can't compare the DataFusionError directly
                    let result = result.to_string();
                    let expected = expected.to_string();
                    assert_contains!(result.to_string(), expected);
                }
                (expected_result, ordering_result) => {
                    panic!(
                        "expected: {expected_result:#?}\n\nactual:{ordering_result:#?}"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn read_empty_table() -> Result<()> {
        let ctx = SessionContext::new();
        let path = String::from("table/p1=v1/file.json");
        register_test_store(&ctx, &[(&path, 100)]);

        let format = JsonFormat::default();
        let ext = format.get_ext();

        let opt = ListingOptions::new(Arc::new(format))
            .with_file_extension(ext)
            .with_table_partition_cols(vec![(String::from("p1"), DataType::Utf8)])
            .with_target_partitions(4);

        let table_path = ListingTableUrl::parse("test:///table/").unwrap();
        let file_schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Boolean, false)]));
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(opt)
            .with_schema(file_schema);
        let table = ListingTable::try_new(config)?;

        assert_eq!(
            columns(&table.schema()),
            vec!["a".to_owned(), "p1".to_owned()]
        );

        // this will filter out the only file in the store
        let filter = Expr::not_eq(col("p1"), lit("v1"));

        let scan = table
            .scan(&ctx.state(), None, &[filter], None)
            .await
            .expect("Empty execution plan");

        assert!(scan.as_any().is::<EmptyExec>());
        assert_eq!(
            columns(&scan.schema()),
            vec!["a".to_owned(), "p1".to_owned()]
        );

        Ok(())
    }

    async fn load_table(
        ctx: &SessionContext,
        name: &str,
    ) -> Result<Arc<dyn TableProvider>> {
        let testdata = crate::test_util::parquet_test_data();
        let filename = format!("{testdata}/{name}");
        let table_path = ListingTableUrl::parse(filename).unwrap();

        let config = ListingTableConfig::new(table_path)
            .infer(&ctx.state())
            .await?;
        let table = ListingTable::try_new(config)?;
        Ok(Arc::new(table))
    }

    /// Check that the files listed by the table match the specified `output_partitioning`
    /// when the object store contains `files`.
    async fn assert_list_files_for_scan_grouping(
        files: &[&str],
        table_prefix: &str,
        target_partitions: usize,
        output_partitioning: usize,
        file_ext: Option<&str>,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        register_test_store(&ctx, &files.iter().map(|f| (*f, 10)).collect::<Vec<_>>());

        let opt = ListingOptions::new(Arc::new(JsonFormat::default()))
            .with_file_extension_opt(file_ext)
            .with_target_partitions(target_partitions);

        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, false)]);

        let table_path = ListingTableUrl::parse(table_prefix).unwrap();
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(opt)
            .with_schema(Arc::new(schema));

        let table = ListingTable::try_new(config)?;

        let (file_list, _) = table.list_files_for_scan(&ctx.state(), &[], None).await?;

        assert_eq!(file_list.len(), output_partitioning);

        Ok(())
    }

    /// Check that the files listed by the table match the specified `output_partitioning`
    /// when the object store contains `files`.
    async fn assert_list_files_for_multi_paths(
        files: &[&str],
        table_prefix: &[&str],
        target_partitions: usize,
        output_partitioning: usize,
        file_ext: Option<&str>,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        register_test_store(&ctx, &files.iter().map(|f| (*f, 10)).collect::<Vec<_>>());

        let opt = ListingOptions::new(Arc::new(JsonFormat::default()))
            .with_file_extension_opt(file_ext)
            .with_target_partitions(target_partitions);

        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, false)]);

        let table_paths = table_prefix
            .iter()
            .map(|t| ListingTableUrl::parse(t).unwrap())
            .collect();
        let config = ListingTableConfig::new_with_multi_paths(table_paths)
            .with_listing_options(opt)
            .with_schema(Arc::new(schema));

        let table = ListingTable::try_new(config)?;

        let (file_list, _) = table.list_files_for_scan(&ctx.state(), &[], None).await?;

        assert_eq!(file_list.len(), output_partitioning);

        Ok(())
    }

    /// Check that the files listed by the table match the specified `output_partitioning`
    /// when the object store contains `files`, and validate that file metadata is fetched
    /// concurrently
    async fn assert_list_files_for_exact_paths(
        files: &[&str],
        target_partitions: usize,
        output_partitioning: usize,
        file_ext: Option<&str>,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        let (store, _) = make_test_store_and_state(
            &files.iter().map(|f| (*f, 10)).collect::<Vec<_>>(),
        );

        let meta_fetch_concurrency = ctx
            .state()
            .config_options()
            .execution
            .meta_fetch_concurrency;
        let expected_concurrency = files.len().min(meta_fetch_concurrency);
        let head_concurrency_store = ensure_head_concurrency(store, expected_concurrency);

        let url = Url::parse("test://").unwrap();
        ctx.register_object_store(&url, head_concurrency_store.clone());

        let format = JsonFormat::default();

        let opt = ListingOptions::new(Arc::new(format))
            .with_file_extension_opt(file_ext)
            .with_target_partitions(target_partitions);

        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, false)]);

        let table_paths = files
            .iter()
            .map(|t| ListingTableUrl::parse(format!("test:///{t}")).unwrap())
            .collect();
        let config = ListingTableConfig::new_with_multi_paths(table_paths)
            .with_listing_options(opt)
            .with_schema(Arc::new(schema));

        let table = ListingTable::try_new(config)?;

        let (file_list, _) = table.list_files_for_scan(&ctx.state(), &[], None).await?;

        assert_eq!(file_list.len(), output_partitioning);

        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_sql_csv_defaults() -> Result<()> {
        helper_test_insert_into_sql("csv", FileCompressionType::UNCOMPRESSED, "", None)
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_sql_csv_defaults_header_row() -> Result<()> {
        helper_test_insert_into_sql(
            "csv",
            FileCompressionType::UNCOMPRESSED,
            "",
            Some(HashMap::from([("has_header".into(), "true".into())])),
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_sql_json_defaults() -> Result<()> {
        helper_test_insert_into_sql("json", FileCompressionType::UNCOMPRESSED, "", None)
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_sql_parquet_defaults() -> Result<()> {
        helper_test_insert_into_sql(
            "parquet",
            FileCompressionType::UNCOMPRESSED,
            "",
            None,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_sql_parquet_session_overrides() -> Result<()> {
        let mut config_map: HashMap<String, String> = HashMap::new();
        config_map.insert(
            "datafusion.execution.parquet.compression".into(),
            "zstd(5)".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.dictionary_enabled".into(),
            "false".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.dictionary_page_size_limit".into(),
            "100".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.statistics_enabled".into(),
            "none".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.max_statistics_size".into(),
            "10".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.max_row_group_size".into(),
            "5".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.created_by".into(),
            "datafusion test".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.column_index_truncate_length".into(),
            "50".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.data_page_row_count_limit".into(),
            "50".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.bloom_filter_on_write".into(),
            "true".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.bloom_filter_fpp".into(),
            "0.01".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.bloom_filter_ndv".into(),
            "1000".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.writer_version".into(),
            "2.0".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.write_batch_size".into(),
            "5".into(),
        );
        helper_test_insert_into_sql(
            "parquet",
            FileCompressionType::UNCOMPRESSED,
            "",
            Some(config_map),
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_append_new_parquet_files_session_overrides() -> Result<()> {
        let mut config_map: HashMap<String, String> = HashMap::new();
        config_map.insert(
            "datafusion.execution.soft_max_rows_per_output_file".into(),
            "10".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.compression".into(),
            "zstd(5)".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.dictionary_enabled".into(),
            "false".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.dictionary_page_size_limit".into(),
            "100".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.statistics_enabled".into(),
            "none".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.max_statistics_size".into(),
            "10".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.max_row_group_size".into(),
            "5".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.created_by".into(),
            "datafusion test".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.column_index_truncate_length".into(),
            "50".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.data_page_row_count_limit".into(),
            "50".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.encoding".into(),
            "delta_binary_packed".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.bloom_filter_on_write".into(),
            "true".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.bloom_filter_fpp".into(),
            "0.01".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.bloom_filter_ndv".into(),
            "1000".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.writer_version".into(),
            "2.0".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.write_batch_size".into(),
            "5".into(),
        );
        config_map.insert("datafusion.execution.batch_size".into(), "10".into());
        helper_test_append_new_files_to_table(
            ParquetFormat::default().get_ext(),
            FileCompressionType::UNCOMPRESSED,
            Some(config_map),
            2,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_append_new_parquet_files_invalid_session_fails(
    ) -> Result<()> {
        let mut config_map: HashMap<String, String> = HashMap::new();
        config_map.insert(
            "datafusion.execution.parquet.compression".into(),
            "zstd".into(),
        );
        let e = helper_test_append_new_files_to_table(
            ParquetFormat::default().get_ext(),
            FileCompressionType::UNCOMPRESSED,
            Some(config_map),
            2,
        )
        .await
        .expect_err("Example should fail!");
        assert_eq!(e.strip_backtrace(), "Invalid or Unsupported Configuration: zstd compression requires specifying a level such as zstd(4)");

        Ok(())
    }

    async fn helper_test_append_new_files_to_table(
        file_type_ext: String,
        file_compression_type: FileCompressionType,
        session_config_map: Option<HashMap<String, String>>,
        expected_n_files_per_insert: usize,
    ) -> Result<()> {
        // Create the initial context, schema, and batch.
        let session_ctx = match session_config_map {
            Some(cfg) => {
                let config = SessionConfig::from_string_hash_map(&cfg)?;
                SessionContext::new_with_config(config)
            }
            None => SessionContext::new(),
        };

        // Create a new schema with one field called "a" of type Int32
        let schema = Arc::new(Schema::new(vec![Field::new(
            "column1",
            DataType::Int32,
            false,
        )]));

        let filter_predicate = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column("column1".into())),
            Operator::GtEq,
            Box::new(Expr::Literal(ScalarValue::Int32(Some(0)), None)),
        ));

        // Create a new batch of data to insert into the table
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow::array::Int32Array::from(vec![
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
            ]))],
        )?;

        // Register appropriate table depending on file_type we want to test
        let tmp_dir = TempDir::new()?;
        match file_type_ext.as_str() {
            "csv" => {
                session_ctx
                    .register_csv(
                        "t",
                        tmp_dir.path().to_str().unwrap(),
                        CsvReadOptions::new()
                            .schema(schema.as_ref())
                            .file_compression_type(file_compression_type),
                    )
                    .await?;
            }
            "json" => {
                session_ctx
                    .register_json(
                        "t",
                        tmp_dir.path().to_str().unwrap(),
                        NdJsonReadOptions::default()
                            .schema(schema.as_ref())
                            .file_compression_type(file_compression_type),
                    )
                    .await?;
            }
            #[cfg(feature = "parquet")]
            "parquet" => {
                session_ctx
                    .register_parquet(
                        "t",
                        tmp_dir.path().to_str().unwrap(),
                        ParquetReadOptions::default().schema(schema.as_ref()),
                    )
                    .await?;
            }
            #[cfg(feature = "avro")]
            "avro" => {
                session_ctx
                    .register_avro(
                        "t",
                        tmp_dir.path().to_str().unwrap(),
                        AvroReadOptions::default().schema(schema.as_ref()),
                    )
                    .await?;
            }
            "arrow" => {
                session_ctx
                    .register_arrow(
                        "t",
                        tmp_dir.path().to_str().unwrap(),
                        ArrowReadOptions::default().schema(schema.as_ref()),
                    )
                    .await?;
            }
            _ => panic!("Unrecognized file extension {file_type_ext}"),
        }

        // Create and register the source table with the provided schema and inserted data
        let source_table = Arc::new(MemTable::try_new(
            schema.clone(),
            vec![vec![batch.clone(), batch.clone()]],
        )?);
        session_ctx.register_table("source", source_table.clone())?;
        // Convert the source table into a provider so that it can be used in a query
        let source = provider_as_source(source_table);
        let target = session_ctx.table_provider("t").await?;
        let target = Arc::new(DefaultTableSource::new(target));
        // Create a table scan logical plan to read from the source table
        let scan_plan = LogicalPlanBuilder::scan("source", source, None)?
            .filter(filter_predicate)?
            .build()?;
        // Since logical plan contains a filter, increasing parallelism is helpful.
        // Therefore, we will have 8 partitions in the final plan.
        // Create an insert plan to insert the source data into the initial table
        let insert_into_table =
            LogicalPlanBuilder::insert_into(scan_plan, "t", target, InsertOp::Append)?
                .build()?;
        // Create a physical plan from the insert plan
        let plan = session_ctx
            .state()
            .create_physical_plan(&insert_into_table)
            .await?;
        // Execute the physical plan and collect the results
        let res = collect(plan, session_ctx.task_ctx()).await?;
        // Insert returns the number of rows written, in our case this would be 6.

        insta::allow_duplicates! {insta::assert_snapshot!(batches_to_string(&res),@r###"
            +-------+
            | count |
            +-------+
            | 20    |
            +-------+
        "###);}

        // Read the records in the table
        let batches = session_ctx
            .sql("select count(*) as count from t")
            .await?
            .collect()
            .await?;

        insta::allow_duplicates! {insta::assert_snapshot!(batches_to_string(&batches),@r###"
            +-------+
            | count |
            +-------+
            | 20    |
            +-------+
        "###);}

        // Assert that `target_partition_number` many files were added to the table.
        let num_files = tmp_dir.path().read_dir()?.count();
        assert_eq!(num_files, expected_n_files_per_insert);

        // Create a physical plan from the insert plan
        let plan = session_ctx
            .state()
            .create_physical_plan(&insert_into_table)
            .await?;

        // Again, execute the physical plan and collect the results
        let res = collect(plan, session_ctx.task_ctx()).await?;

        insta::allow_duplicates! {insta::assert_snapshot!(batches_to_string(&res),@r###"
            +-------+
            | count |
            +-------+
            | 20    |
            +-------+
        "###);}

        // Read the contents of the table
        let batches = session_ctx
            .sql("select count(*) AS count from t")
            .await?
            .collect()
            .await?;

        insta::allow_duplicates! {insta::assert_snapshot!(batches_to_string(&batches),@r###"
            +-------+
            | count |
            +-------+
            | 40    |
            +-------+
        "###);}

        // Assert that another `target_partition_number` many files were added to the table.
        let num_files = tmp_dir.path().read_dir()?.count();
        assert_eq!(num_files, expected_n_files_per_insert * 2);

        // Return Ok if the function
        Ok(())
    }

    /// tests insert into with end to end sql
    /// create external table + insert into statements
    async fn helper_test_insert_into_sql(
        file_type: &str,
        // TODO test with create statement options such as compression
        _file_compression_type: FileCompressionType,
        external_table_options: &str,
        session_config_map: Option<HashMap<String, String>>,
    ) -> Result<()> {
        // Create the initial context
        let session_ctx = match session_config_map {
            Some(cfg) => {
                let config = SessionConfig::from_string_hash_map(&cfg)?;
                SessionContext::new_with_config(config)
            }
            None => SessionContext::new(),
        };

        // create table
        let tmp_dir = TempDir::new()?;
        let str_path = tmp_dir
            .path()
            .to_str()
            .expect("Temp path should convert to &str");
        session_ctx
            .sql(&format!(
                "create external table foo(a varchar, b varchar, c int) \
                        stored as {file_type} \
                        location '{str_path}' \
                        {external_table_options}"
            ))
            .await?
            .collect()
            .await?;

        // insert data
        session_ctx.sql("insert into foo values ('foo', 'bar', 1),('foo', 'bar', 2), ('foo', 'bar', 3)")
            .await?
            .collect()
            .await?;

        // check count
        let batches = session_ctx
            .sql("select * from foo")
            .await?
            .collect()
            .await?;

        insta::allow_duplicates! {insta::assert_snapshot!(batches_to_string(&batches),@r###"
            +-----+-----+---+
            | a   | b   | c |
            +-----+-----+---+
            | foo | bar | 1 |
            | foo | bar | 2 |
            | foo | bar | 3 |
            +-----+-----+---+
        "###);}

        Ok(())
    }

    #[tokio::test]
    async fn test_infer_options_compressed_csv() -> Result<()> {
        let testdata = crate::test_util::arrow_test_data();
        let filename = format!("{testdata}/csv/aggregate_test_100.csv.gz");
        let table_path = ListingTableUrl::parse(filename).unwrap();

        let ctx = SessionContext::new();

        let config = ListingTableConfig::new(table_path);
        let config_with_opts = config.infer_options(&ctx.state()).await?;
        let config_with_schema = config_with_opts.infer_schema(&ctx.state()).await?;

        let schema = config_with_schema.file_schema.unwrap();

        assert_eq!(schema.fields.len(), 13);

        Ok(())
    }

    #[tokio::test]
    async fn infer_preserves_provided_schema() -> Result<()> {
        let ctx = SessionContext::new();

        let testdata = datafusion_test_data();
        let filename = format!("{testdata}/aggregate_simple.csv");
        let table_path = ListingTableUrl::parse(filename).unwrap();

        let provided_schema = create_test_schema();

        let config =
            ListingTableConfig::new(table_path).with_schema(Arc::clone(&provided_schema));

        let config = config.infer(&ctx.state()).await?;

        assert_eq!(*config.file_schema.unwrap(), *provided_schema);

        Ok(())
    }

    #[tokio::test]
    async fn test_listing_table_config_with_multiple_files_comprehensive() -> Result<()> {
        let ctx = SessionContext::new();

        // Create test files with different schemas
        let tmp_dir = TempDir::new()?;
        let file_path1 = tmp_dir.path().join("file1.csv");
        let file_path2 = tmp_dir.path().join("file2.csv");

        // File 1: c1,c2,c3
        let mut file1 = std::fs::File::create(&file_path1)?;
        writeln!(file1, "c1,c2,c3")?;
        writeln!(file1, "1,2,3")?;
        writeln!(file1, "4,5,6")?;

        // File 2: c1,c2,c3,c4
        let mut file2 = std::fs::File::create(&file_path2)?;
        writeln!(file2, "c1,c2,c3,c4")?;
        writeln!(file2, "7,8,9,10")?;
        writeln!(file2, "11,12,13,14")?;

        // Parse paths
        let table_path1 = ListingTableUrl::parse(file_path1.to_str().unwrap())?;
        let table_path2 = ListingTableUrl::parse(file_path2.to_str().unwrap())?;

        // Create format and options
        let format = CsvFormat::default().with_has_header(true);
        let options = ListingOptions::new(Arc::new(format));

        // Test case 1: Infer schema using first file's schema
        let config1 = ListingTableConfig::new_with_multi_paths(vec![
            table_path1.clone(),
            table_path2.clone(),
        ])
        .with_listing_options(options.clone());
        let config1 = config1.infer_schema(&ctx.state()).await?;
        assert_eq!(config1.schema_source(), SchemaSource::Inferred);

        // Verify schema matches first file
        let schema1 = config1.file_schema.as_ref().unwrap().clone();
        assert_eq!(schema1.fields().len(), 3);
        assert_eq!(schema1.field(0).name(), "c1");
        assert_eq!(schema1.field(1).name(), "c2");
        assert_eq!(schema1.field(2).name(), "c3");

        // Test case 2: Use specified schema with 3 columns
        let schema_3cols = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::Utf8, true),
            Field::new("c3", DataType::Utf8, true),
        ]));

        let config2 = ListingTableConfig::new_with_multi_paths(vec![
            table_path1.clone(),
            table_path2.clone(),
        ])
        .with_schema(schema_3cols)
        .with_listing_options(options.clone());
        let config2 = config2.infer_schema(&ctx.state()).await?;
        assert_eq!(config2.schema_source(), SchemaSource::Specified);

        // Verify that the schema is still the one we specified (3 columns)
        let schema2 = config2.file_schema.as_ref().unwrap().clone();
        assert_eq!(schema2.fields().len(), 3);
        assert_eq!(schema2.field(0).name(), "c1");
        assert_eq!(schema2.field(1).name(), "c2");
        assert_eq!(schema2.field(2).name(), "c3");

        // Test case 3: Use specified schema with 4 columns
        let schema_4cols = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::Utf8, true),
            Field::new("c3", DataType::Utf8, true),
            Field::new("c4", DataType::Utf8, true),
        ]));

        let config3 = ListingTableConfig::new_with_multi_paths(vec![
            table_path1.clone(),
            table_path2.clone(),
        ])
        .with_schema(schema_4cols)
        .with_listing_options(options.clone());
        let config3 = config3.infer_schema(&ctx.state()).await?;
        assert_eq!(config3.schema_source(), SchemaSource::Specified);

        // Verify that the schema is still the one we specified (4 columns)
        let schema3 = config3.file_schema.as_ref().unwrap().clone();
        assert_eq!(schema3.fields().len(), 4);
        assert_eq!(schema3.field(0).name(), "c1");
        assert_eq!(schema3.field(1).name(), "c2");
        assert_eq!(schema3.field(2).name(), "c3");
        assert_eq!(schema3.field(3).name(), "c4");

        // Test case 4: Verify order matters when inferring schema
        let config4 = ListingTableConfig::new_with_multi_paths(vec![
            table_path2.clone(),
            table_path1.clone(),
        ])
        .with_listing_options(options);
        let config4 = config4.infer_schema(&ctx.state()).await?;

        // Should use first file's schema, which now has 4 columns
        let schema4 = config4.file_schema.as_ref().unwrap().clone();
        assert_eq!(schema4.fields().len(), 4);
        assert_eq!(schema4.field(0).name(), "c1");
        assert_eq!(schema4.field(1).name(), "c2");
        assert_eq!(schema4.field(2).name(), "c3");
        assert_eq!(schema4.field(3).name(), "c4");

        Ok(())
    }

    #[tokio::test]
    async fn test_list_files_configurations() -> Result<()> {
        // Define common test cases as (description, files, paths, target_partitions, expected_partitions, file_ext)
        let test_cases = vec![
            // Single path cases
            (
                "Single path, more partitions than files",
                generate_test_files("bucket/key-prefix", 5),
                vec!["test:///bucket/key-prefix/"],
                12,
                5,
                Some(""),
            ),
            (
                "Single path, equal partitions and files",
                generate_test_files("bucket/key-prefix", 4),
                vec!["test:///bucket/key-prefix/"],
                4,
                4,
                Some(""),
            ),
            (
                "Single path, more files than partitions",
                generate_test_files("bucket/key-prefix", 5),
                vec!["test:///bucket/key-prefix/"],
                2,
                2,
                Some(""),
            ),
            // Multi path cases
            (
                "Multi path, more partitions than files",
                {
                    let mut files = generate_test_files("bucket/key1", 3);
                    files.extend(generate_test_files_with_start("bucket/key2", 2, 3));
                    files.extend(generate_test_files_with_start("bucket/key3", 1, 5));
                    files
                },
                vec!["test:///bucket/key1/", "test:///bucket/key2/"],
                12,
                5,
                Some(""),
            ),
            // No files case
            (
                "No files",
                vec![],
                vec!["test:///bucket/key-prefix/"],
                2,
                0,
                Some(""),
            ),
            // Exact path cases
            (
                "Exact paths test",
                {
                    let mut files = generate_test_files("bucket/key1", 3);
                    files.extend(generate_test_files_with_start("bucket/key2", 2, 3));
                    files
                },
                vec![
                    "test:///bucket/key1/file0",
                    "test:///bucket/key1/file1",
                    "test:///bucket/key1/file2",
                    "test:///bucket/key2/file3",
                    "test:///bucket/key2/file4",
                ],
                12,
                5,
                Some(""),
            ),
        ];

        // Run each test case
        for (test_name, files, paths, target_partitions, expected_partitions, file_ext) in
            test_cases
        {
            println!("Running test: {test_name}");

            if files.is_empty() {
                // Test empty files case
                assert_list_files_for_multi_paths(
                    &[],
                    &paths,
                    target_partitions,
                    expected_partitions,
                    file_ext,
                )
                .await?;
            } else if paths.len() == 1 {
                // Test using single path API
                let file_refs: Vec<&str> = files.iter().map(|s| s.as_str()).collect();
                assert_list_files_for_scan_grouping(
                    &file_refs,
                    paths[0],
                    target_partitions,
                    expected_partitions,
                    file_ext,
                )
                .await?;
            } else if paths[0].contains("test:///bucket/key") {
                // Test using multi path API
                let file_refs: Vec<&str> = files.iter().map(|s| s.as_str()).collect();
                assert_list_files_for_multi_paths(
                    &file_refs,
                    &paths,
                    target_partitions,
                    expected_partitions,
                    file_ext,
                )
                .await?;
            } else {
                // Test using exact path API for specific cases
                let file_refs: Vec<&str> = files.iter().map(|s| s.as_str()).collect();
                assert_list_files_for_exact_paths(
                    &file_refs,
                    target_partitions,
                    expected_partitions,
                    file_ext,
                )
                .await?;
            }
        }

        Ok(())
    }

    #[cfg(feature = "parquet")]
    #[tokio::test]
    async fn test_table_stats_behaviors() -> Result<()> {
        use crate::datasource::file_format::parquet::ParquetFormat;

        let testdata = crate::test_util::parquet_test_data();
        let filename = format!("{}/{}", testdata, "alltypes_plain.parquet");
        let table_path = ListingTableUrl::parse(filename).unwrap();

        let ctx = SessionContext::new();
        let state = ctx.state();

        // Test 1: Default behavior - stats not collected
        let opt_default = ListingOptions::new(Arc::new(ParquetFormat::default()));
        let schema_default = opt_default.infer_schema(&state, &table_path).await?;
        let config_default = ListingTableConfig::new(table_path.clone())
            .with_listing_options(opt_default)
            .with_schema(schema_default);
        let table_default = ListingTable::try_new(config_default)?;

        let exec_default = table_default.scan(&state, None, &[], None).await?;
        assert_eq!(
            exec_default.partition_statistics(None)?.num_rows,
            Precision::Absent
        );

        // TODO correct byte size: https://github.com/apache/datafusion/issues/14936
        assert_eq!(
            exec_default.partition_statistics(None)?.total_byte_size,
            Precision::Absent
        );

        // Test 2: Explicitly disable stats
        let opt_disabled = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_collect_stat(false);
        let schema_disabled = opt_disabled.infer_schema(&state, &table_path).await?;
        let config_disabled = ListingTableConfig::new(table_path.clone())
            .with_listing_options(opt_disabled)
            .with_schema(schema_disabled);
        let table_disabled = ListingTable::try_new(config_disabled)?;

        let exec_disabled = table_disabled.scan(&state, None, &[], None).await?;
        assert_eq!(
            exec_disabled.partition_statistics(None)?.num_rows,
            Precision::Absent
        );
        assert_eq!(
            exec_disabled.partition_statistics(None)?.total_byte_size,
            Precision::Absent
        );

        // Test 3: Explicitly enable stats
        let opt_enabled = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_collect_stat(true);
        let schema_enabled = opt_enabled.infer_schema(&state, &table_path).await?;
        let config_enabled = ListingTableConfig::new(table_path)
            .with_listing_options(opt_enabled)
            .with_schema(schema_enabled);
        let table_enabled = ListingTable::try_new(config_enabled)?;

        let exec_enabled = table_enabled.scan(&state, None, &[], None).await?;
        assert_eq!(
            exec_enabled.partition_statistics(None)?.num_rows,
            Precision::Exact(8)
        );
        // TODO correct byte size: https://github.com/apache/datafusion/issues/14936
        assert_eq!(
            exec_enabled.partition_statistics(None)?.total_byte_size,
            Precision::Exact(671)
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_parameterized() -> Result<()> {
        let test_cases = vec![
            // (file_format, batch_size, soft_max_rows, expected_files)
            ("json", 10, 10, 2),
            ("csv", 10, 10, 2),
            #[cfg(feature = "parquet")]
            ("parquet", 10, 10, 2),
            #[cfg(feature = "parquet")]
            ("parquet", 20, 20, 1),
        ];

        for (format, batch_size, soft_max_rows, expected_files) in test_cases {
            println!("Testing insert with format: {format}, batch_size: {batch_size}, expected files: {expected_files}");

            let mut config_map = HashMap::new();
            config_map.insert(
                "datafusion.execution.batch_size".into(),
                batch_size.to_string(),
            );
            config_map.insert(
                "datafusion.execution.soft_max_rows_per_output_file".into(),
                soft_max_rows.to_string(),
            );

            let file_extension = match format {
                "json" => JsonFormat::default().get_ext(),
                "csv" => CsvFormat::default().get_ext(),
                #[cfg(feature = "parquet")]
                "parquet" => ParquetFormat::default().get_ext(),
                _ => unreachable!("Unsupported format"),
            };

            helper_test_append_new_files_to_table(
                file_extension,
                FileCompressionType::UNCOMPRESSED,
                Some(config_map),
                expected_files,
            )
            .await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_statistics_mapping_with_custom_factory() -> Result<()> {
        let ctx = SessionContext::new();
        let table = create_test_listing_table_with_json_and_adapter(
            &ctx,
            false,
            // NullStatsAdapterFactory sets column_statistics null_count to DUMMY_NULL_COUNT
            Arc::new(NullStatsAdapterFactory {}),
        )?;

        let (groups, stats) = table.list_files_for_scan(&ctx.state(), &[], None).await?;

        assert_eq!(stats.column_statistics[0].null_count, DUMMY_NULL_COUNT);
        for g in groups {
            if let Some(s) = g.file_statistics(None) {
                assert_eq!(s.column_statistics[0].null_count, DUMMY_NULL_COUNT);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_statistics_mapping_with_default_factory() -> Result<()> {
        let ctx = SessionContext::new();

        // Create a table without providing a custom schema adapter factory
        // This should fall back to using DefaultSchemaAdapterFactory
        let path = "table/file.json";
        register_test_store(&ctx, &[(path, 10)]);

        let format = JsonFormat::default();
        let opt = ListingOptions::new(Arc::new(format)).with_collect_stat(false);
        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, false)]);
        let table_path = ListingTableUrl::parse("test:///table/").unwrap();

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(opt)
            .with_schema(Arc::new(schema));
        // Note: NOT calling .with_schema_adapter_factory() to test default behavior

        let table = ListingTable::try_new(config)?;

        // Verify that no custom schema adapter factory is set
        assert!(table.schema_adapter_factory().is_none());

        // The scan should work correctly with the default schema adapter
        let scan_result = table.scan(&ctx.state(), None, &[], None).await;
        assert!(
            scan_result.is_ok(),
            "Scan should succeed with default schema adapter"
        );

        // Verify that the default adapter handles basic schema compatibility
        let (groups, _stats) = table.list_files_for_scan(&ctx.state(), &[], None).await?;
        assert!(
            !groups.is_empty(),
            "Should list files successfully with default adapter"
        );

        Ok(())
    }

    #[rstest]
    #[case(MapSchemaError::TypeIncompatible, "Cannot map incompatible types")]
    #[case(MapSchemaError::GeneralFailure, "Schema adapter mapping failed")]
    #[case(
        MapSchemaError::InvalidProjection,
        "Invalid projection in schema mapping"
    )]
    #[tokio::test]
    async fn test_schema_adapter_map_schema_errors(
        #[case] error_type: MapSchemaError,
        #[case] expected_error_msg: &str,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        let table = create_test_listing_table_with_json_and_adapter(
            &ctx,
            false,
            Arc::new(FailingMapSchemaAdapterFactory { error_type }),
        )?;

        // The error should bubble up from the scan operation when schema mapping fails
        let scan_result = table.scan(&ctx.state(), None, &[], None).await;

        assert!(scan_result.is_err());
        let error_msg = scan_result.unwrap_err().to_string();
        assert!(
            error_msg.contains(expected_error_msg),
            "Expected error containing '{expected_error_msg}', got: {error_msg}"
        );

        Ok(())
    }

    // Test that errors during file listing also bubble up correctly
    #[tokio::test]
    async fn test_schema_adapter_error_during_file_listing() -> Result<()> {
        let ctx = SessionContext::new();
        let table = create_test_listing_table_with_json_and_adapter(
            &ctx,
            true,
            Arc::new(FailingMapSchemaAdapterFactory {
                error_type: MapSchemaError::TypeIncompatible,
            }),
        )?;

        // The error should bubble up from list_files_for_scan when collecting statistics
        let list_result = table.list_files_for_scan(&ctx.state(), &[], None).await;

        assert!(list_result.is_err());
        let error_msg = list_result.unwrap_err().to_string();
        assert!(
            error_msg.contains("Cannot map incompatible types"),
            "Expected type incompatibility error during file listing, got: {error_msg}"
        );

        Ok(())
    }

    #[derive(Debug, Copy, Clone)]
    enum MapSchemaError {
        TypeIncompatible,
        GeneralFailure,
        InvalidProjection,
    }

    #[derive(Debug)]
    struct FailingMapSchemaAdapterFactory {
        error_type: MapSchemaError,
    }

    impl SchemaAdapterFactory for FailingMapSchemaAdapterFactory {
        fn create(
            &self,
            projected_table_schema: SchemaRef,
            _table_schema: SchemaRef,
        ) -> Box<dyn SchemaAdapter> {
            Box::new(FailingMapSchemaAdapter {
                schema: projected_table_schema,
                error_type: self.error_type,
            })
        }
    }

    #[derive(Debug)]
    struct FailingMapSchemaAdapter {
        schema: SchemaRef,
        error_type: MapSchemaError,
    }

    impl SchemaAdapter for FailingMapSchemaAdapter {
        fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
            let field = self.schema.field(index);
            file_schema.fields.find(field.name()).map(|(i, _)| i)
        }

        fn map_schema(
            &self,
            _file_schema: &Schema,
        ) -> Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
            // Always fail with different error types based on the configured error_type
            match self.error_type {
                MapSchemaError::TypeIncompatible => {
                    plan_err!(
                        "Cannot map incompatible types: Boolean cannot be cast to Utf8"
                    )
                }
                MapSchemaError::GeneralFailure => {
                    plan_err!("Schema adapter mapping failed due to internal error")
                }
                MapSchemaError::InvalidProjection => {
                    plan_err!("Invalid projection in schema mapping: column index out of bounds")
                }
            }
        }
    }

    #[derive(Debug)]
    struct NullStatsAdapterFactory;

    impl SchemaAdapterFactory for NullStatsAdapterFactory {
        fn create(
            &self,
            projected_table_schema: SchemaRef,
            _table_schema: SchemaRef,
        ) -> Box<dyn SchemaAdapter> {
            Box::new(NullStatsAdapter {
                schema: projected_table_schema,
            })
        }
    }

    #[derive(Debug)]
    struct NullStatsAdapter {
        schema: SchemaRef,
    }

    impl SchemaAdapter for NullStatsAdapter {
        fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
            let field = self.schema.field(index);
            file_schema.fields.find(field.name()).map(|(i, _)| i)
        }

        fn map_schema(
            &self,
            file_schema: &Schema,
        ) -> Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
            let projection = (0..file_schema.fields().len()).collect();
            Ok((Arc::new(NullStatsMapper {}), projection))
        }
    }

    #[derive(Debug)]
    struct NullStatsMapper;

    impl SchemaMapper for NullStatsMapper {
        fn map_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
            Ok(batch)
        }

        fn map_column_statistics(
            &self,
            stats: &[ColumnStatistics],
        ) -> Result<Vec<ColumnStatistics>> {
            Ok(stats
                .iter()
                .map(|s| {
                    let mut s = s.clone();
                    s.null_count = DUMMY_NULL_COUNT;
                    s
                })
                .collect())
        }
    }

    /// Helper function to create a test ListingTable with JSON format and custom schema adapter factory
    fn create_test_listing_table_with_json_and_adapter(
        ctx: &SessionContext,
        collect_stat: bool,
        schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Result<ListingTable> {
        let path = "table/file.json";
        register_test_store(ctx, &[(path, 10)]);

        let format = JsonFormat::default();
        let opt = ListingOptions::new(Arc::new(format)).with_collect_stat(collect_stat);
        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, false)]);
        let table_path = ListingTableUrl::parse("test:///table/").unwrap();

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(opt)
            .with_schema(Arc::new(schema))
            .with_schema_adapter_factory(schema_adapter_factory);

        ListingTable::try_new(config)
    }
}
