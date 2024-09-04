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

extern crate criterion;

use arrow::array::{ArrayRef, StringArray, StringViewBuilder};
use arrow::util::bench_util::{
    create_string_array_with_len, create_string_view_array_with_len,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_expr::ColumnarValue;
use std::sync::Arc;

/// Create an array of args containing a StringArray, where all the values in the
/// StringArray are ASCII.
/// * `size` - the length of the StringArray, and
/// * `str_len` - the length of the strings within the StringArray.
fn create_args1(size: usize, str_len: usize) -> Vec<ColumnarValue> {
    let array = Arc::new(create_string_array_with_len::<i32>(size, 0.2, str_len));
    vec![ColumnarValue::Array(array)]
}

/// Create an array of args containing a StringArray, where the first value in the
/// StringArray is non-ASCII.
/// * `size` - the length of the StringArray, and
/// * `str_len` - the length of the strings within the StringArray.
fn create_args2(size: usize) -> Vec<ColumnarValue> {
    let mut items = Vec::with_capacity(size);
    items.push("农历新年".to_string());
    for i in 1..size {
        items.push(format!("DATAFUSION {:0128}", i));
    }
    let array = Arc::new(StringArray::from(items)) as ArrayRef;
    vec![ColumnarValue::Array(array)]
}

/// Create an array of args containing a StringArray, where the middle value of the
/// StringArray is non-ASCII.
/// * `size` - the length of the StringArray, and
/// * `str_len` - the length of the strings within the StringArray.
fn create_args3(size: usize) -> Vec<ColumnarValue> {
    let mut items = Vec::with_capacity(size);
    let half = size / 2;
    for i in 0..half {
        items.push(format!("DATAFUSION {}", i));
    }
    items.push("Ⱦ".to_string());
    for i in half + 1..size {
        items.push(format!("DATAFUSION {}", i));
    }
    let array = Arc::new(StringArray::from(items)) as ArrayRef;
    vec![ColumnarValue::Array(array)]
}

/// Create an array of args containing StringViews, where all the values in the
/// StringViews are ASCII.
/// * `size` - the length of the StringViews, and
/// * `str_len` - the length of the strings within the array.
/// * `null_density` - the density of null values in the array.
/// * `mixed` - whether the array is mixed between inlined and referenced strings.
fn create_args4(
    size: usize,
    str_len: usize,
    null_density: f32,
    mixed: bool,
) -> Vec<ColumnarValue> {
    let array = Arc::new(create_string_view_array_with_len(
        size,
        null_density,
        str_len,
        mixed,
    ));

    vec![ColumnarValue::Array(array)]
}

/// Create an array of args containing a StringViewArray, where some of the values in the
/// array are non-ASCII.
/// * `size` - the length of the StringArray, and
/// * `non_ascii_density` - the density of non-ASCII values in the array.
/// * `null_density` - the density of null values in the array.
fn create_args5(
    size: usize,
    non_ascii_density: f32,
    null_density: f32,
) -> Vec<ColumnarValue> {
    let mut string_view_builder = StringViewBuilder::with_capacity(size);
    for _ in 0..size {
        // sample null_density to determine if the value should be null
        if rand::random::<f32>() < null_density {
            string_view_builder.append_null();
            continue;
        }

        // sample non_ascii_density to determine if the value should be non-ASCII
        if rand::random::<f32>() < non_ascii_density {
            string_view_builder.append_value(&format!(
                "{:0128}",
                "农历新年农历新年农历新年农历新年农历新年"
            ));
        } else {
            string_view_builder
                .append_value(&format!("{:0128}", "DATAFUSIONDATAFUSIONDATAFUSION"));
        }
    }

    let array = Arc::new(string_view_builder.finish()) as ArrayRef;
    vec![ColumnarValue::Array(array)]
}

fn criterion_benchmark(c: &mut Criterion) {
    // All benches are single batch run with 8192 rows
    let character_length = datafusion_functions::unicode::character_length();
    let size = 8192;

    // StringArray ASCII Only
    let args_str_len_8 = create_args1(size, 8);
    c.bench_function(
        &format!("character_length_StringArray_ascii_str_len_8: {}", size),
        |b| b.iter(|| black_box(character_length.invoke(&args_str_len_8))),
    );

    let args_str_len_128 = create_args1(size, 128);
    c.bench_function(
        &format!("character_length_StringArray_ascii_str_len_128: {}", size),
        |b| b.iter(|| black_box(character_length.invoke(&args_str_len_128))),
    );

    // StringArray with non-ASCII
    let args = create_args2(size);
    c.bench_function(
        &format!(
            "character_length_StringArray_non_ascii_str_len_128: {}",
            size
        ),
        |b| b.iter(|| black_box(character_length.invoke(&args))),
    );

    // StringViewArray with ASCII only
    let args_str_len_8 = create_args4(size, 8, 0.0, true);
    c.bench_function(
        &format!("character_length_StringViewArray_ascii_str_len_8: {}", size),
        |b| b.iter(|| black_box(character_length.invoke(&args_str_len_8))),
    );

    let args_str_len_128 = create_args4(size, 128, 0.0, true);
    c.bench_function(
        &format!(
            "character_length_StringViewArray_ascii_str_len_128: {}",
            size
        ),
        |b| b.iter(|| black_box(character_length.invoke(&args_str_len_128))),
    );

    // StringViewArray with non-ASCII
    let args = create_args5(size, 0.1, 0.1);
    c.bench_function(
        &format!(
            "character_length_StringViewArray_non_ascii_str_len_128: {}",
            size
        ),
        |b| b.iter(|| black_box(character_length.invoke(&args))),
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
