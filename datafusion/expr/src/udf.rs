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

//! Udf module contains foundational types that are used to represent UDFs in DataFusion.

use crate::{
    ColumnarValue, Expr, ReturnTypeFunction, ScalarFunctionImplementation, Signature,
    Volatility,
};
use arrow::array::{ArrayRef, Float32Array, Float64Array};
use arrow::datatypes::DataType;
use datafusion_common::cast::as_float64_array;
use datafusion_common::{Result, ScalarValue};
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

pub trait ScalarFunctionDef: Sync + Send + std::fmt::Debug {
    // TODO: support alias
    fn name(&self) -> &str;

    fn signature(&self) -> Signature;

    // TODO: ReturnTypeFunction -> a ENUM
    //     most function's return type is either the same as 1st arg or a fixed type
    fn return_type(&self) -> ReturnTypeFunction;

    fn execute(&self, args: &[ArrayRef]) -> Result<ArrayRef>;
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Hint {
    /// Indicates the argument needs to be padded if it is scalar
    Pad,
    /// Indicates the argument can be converted to an array of length 1
    AcceptsSingular,
}

/// decorates a function to handle [`ScalarValue`]s by converting them to arrays before calling the function
/// and vice-versa after evaluation.
pub fn make_scalar_function<F>(inner: F) -> ScalarFunctionImplementation
where
    F: Fn(&[ArrayRef]) -> Result<ArrayRef> + Sync + Send + 'static,
{
    make_scalar_function_with_hints(inner, vec![])
}

/// Just like [`make_scalar_function`], decorates the given function to handle both [`ScalarValue`]s and arrays.
/// Additionally can receive a `hints` vector which can be used to control the output arrays when generating them
/// from [`ScalarValue`]s.
///
/// Each element of the `hints` vector gets mapped to the corresponding argument of the function. The number of hints
/// can be less or greater than the number of arguments (for functions with variable number of arguments). Each unmapped
/// argument will assume the default hint (for padding, it is [`Hint::Pad`]).
pub(crate) fn make_scalar_function_with_hints<F>(
    inner: F,
    hints: Vec<Hint>,
) -> ScalarFunctionImplementation
where
    F: Fn(&[ArrayRef]) -> Result<ArrayRef> + Sync + Send + 'static,
{
    Arc::new(move |args: &[ColumnarValue]| {
        // first, identify if any of the arguments is an Array. If yes, store its `len`,
        // as any scalar will need to be converted to an array of len `len`.
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let inferred_length = len.unwrap_or(1);
        let args = args
            .iter()
            .zip(hints.iter().chain(std::iter::repeat(&Hint::Pad)))
            .map(|(arg, hint)| {
                // Decide on the length to expand this scalar to depending
                // on the given hints.
                let expansion_len = match hint {
                    Hint::AcceptsSingular => 1,
                    Hint::Pad => inferred_length,
                };
                arg.clone().into_array(expansion_len)
            })
            .collect::<Vec<ArrayRef>>();

        let result = (inner)(&args);

        // maybe back to scalar
        if len.is_some() {
            result.map(ColumnarValue::Array)
        } else {
            ScalarValue::try_from_array(&result?, 0).map(ColumnarValue::Scalar)
        }
    })
}

/// Make a `dyn ScalarFunctionDef` into a internal struct for scalar functions, then it can be
/// registered into context
pub fn to_scalar_function(func: Arc<dyn ScalarFunctionDef>) -> ScalarUDF {
    let execution = Arc::clone(&func);
    let func_impl = make_scalar_function(move |args| execution.execute(args));

    ScalarUDF::new(
        func.name(),
        &func.signature(),
        &func.return_type(),
        &func_impl,
    )
}

#[derive(Debug)]
pub struct PowFunction;

impl ScalarFunctionDef for PowFunction {
    fn name(&self) -> &str {
        return "powr";
    }

    fn signature(&self) -> Signature {
        Signature::exact(
            vec![DataType::Float64, DataType::Float64],
            Volatility::Immutable,
        )
    }

    fn return_type(&self) -> ReturnTypeFunction {
        let return_type = Arc::new(DataType::Float64);
        let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(return_type.clone()));
        return return_type;
    }

    fn execute(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        assert_eq!(args.len(), 2);
        let base = as_float64_array(&args[0]).expect("cast failed");
        let exponent = as_float64_array(&args[1]).expect("cast failed");
        assert_eq!(exponent.len(), base.len());
        let array = base
            .iter()
            .zip(exponent.iter())
            .map(|(base, exponent)| match (base, exponent) {
                (Some(base), Some(exponent)) => Some(base.powf(exponent)),
                _ => None,
            })
            .collect::<Float64Array>();
        Ok(Arc::new(array) as ArrayRef)
    }
}

/// Logical representation of a UDF.
#[derive(Clone)]
pub struct ScalarUDF {
    /// name
    pub name: String,
    /// signature
    pub signature: Signature,
    /// Return type
    pub return_type: ReturnTypeFunction,
    /// actual implementation
    ///
    /// The fn param is the wrapped function but be aware that the function will
    /// be passed with the slice / vec of columnar values (either scalar or array)
    /// with the exception of zero param function, where a singular element vec
    /// will be passed. In that case the single element is a null array to indicate
    /// the batch's row count (so that the generative zero-argument function can know
    /// the result array size).
    pub fun: ScalarFunctionImplementation,
}

impl Debug for ScalarUDF {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("ScalarUDF")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .field("fun", &"<FUNC>")
            .finish()
    }
}

impl PartialEq for ScalarUDF {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.signature == other.signature
    }
}

impl Eq for ScalarUDF {}

impl std::hash::Hash for ScalarUDF {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.signature.hash(state);
    }
}

impl ScalarUDF {
    /// Create a new ScalarUDF
    pub fn new(
        name: &str,
        signature: &Signature,
        return_type: &ReturnTypeFunction,
        fun: &ScalarFunctionImplementation,
    ) -> Self {
        Self {
            name: name.to_owned(),
            signature: signature.clone(),
            return_type: return_type.clone(),
            fun: fun.clone(),
        }
    }

    /// creates a logical expression with a call of the UDF
    /// This utility allows using the UDF without requiring access to the registry.
    pub fn call(&self, args: Vec<Expr>) -> Expr {
        Expr::ScalarUDF(crate::expr::ScalarUDF::new(Arc::new(self.clone()), args))
    }
}
