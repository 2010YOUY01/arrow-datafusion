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

use datafusion::arrow::array::{UInt64Array, UInt8Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{assert_batches_eq, exec_datafusion_err};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::*;
use object_store::local::LocalFileSystem;
use std::path::Path;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let random_seed = 310106;
    let test_generator = SortFuzzerTestGenerator::new(
        10000,
        4,
        "sort_fuzz_table".to_string(),
        get_supported_types_columns(random_seed),
        random_seed,
    );
    let mut fuzzer = SortQueryFuzzer::new(random_seed)
        .with_max_rounds(Some(5))
        .with_queries_per_round(4)
        .with_config_variations_per_query(25)
        .with_time_limit(Duration::from_secs(60 * 60))
        .with_test_generator(test_generator);

    fuzzer.run().await.unwrap();

    Ok(())
}
