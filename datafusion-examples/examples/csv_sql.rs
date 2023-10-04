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

use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::error::Result;
use datafusion::prelude::*;
use std::time::Instant;

/// This example demonstrates executing a simple query against an Arrow data source (CSV) and
/// fetching results
#[tokio::main]
async fn main() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();
    let df = ctx
        .sql(
            "CREATE EXTERNAL TABLE lineitem STORED AS PARQUET LOCATION '/Users/yongting/Desktop/code/my_datafusion/arrow-datafusion/benchmarks/data/tpch_sf10/lineitem/part-0.parquet';",
        )
        .await?;
    df.show().await?;

    let start = Instant::now(); // Records the current time
    let df = ctx.sql("select * from lineitem;").await?;
    df.show().await?;

    let duration = start.elapsed(); // Calculates the elapsed time
    println!("Time elapsed in milliseconds: {:?}", duration.as_millis());
    Ok(())
}
