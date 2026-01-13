use pyo3::prelude::*;
use pyo3_asyncio::tokio::future_into_py;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::eth::BlockNumberOrTag;
use url::Url;
use arrow::array::{UInt64Builder, StringBuilder}; 
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc; 


/// Asynchronously fetches a block from the Ethereum blockchain and returns its data
#[pyfunction]
fn fetch_block_arrow(py: Python<'_>, rpc_url: String, block_number: u64) -> PyResult<&PyAny> {
    future_into_py(py, async move {
        // -- SETUP CONNECTION -- 
        let valid_url = Url::parse(&rpc_url)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid URL: {}", e)))?;

        let provider = ProviderBuilder::new()
            .on_http(valid_url);

        // -- FETCH BLOCK DATA --
        let alloy_block_number = BlockNumberOrTag::Number(block_number);

        let block_data = provider.get_block_by_number(alloy_block_number, true)
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to get block data: {}", e)))?
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Block not found".to_string()))?;

        // -- DEFINE SCHEMA --
        let schema = Schema::new(vec![
            Field::new("number", DataType::UInt64, false), 
            Field::new("hash", DataType::Utf8, false),
            Field::new("parent_hash", DataType::Utf8, false),
        ]);

        // -- INITIALIZE BUILDERS --
        let mut number_builder = UInt64Builder::new();
        let mut hash_builder = StringBuilder::new();
        let mut parent_builder = StringBuilder::new();


        // -- POPULATE BUILDERS --
        number_builder.append_value(block_data.header.number
            .unwrap_or_default());
        hash_builder.append_value(
            block_data.header.hash
            .map(|h| h.to_string())
            .unwrap_or_default()
        );

        parent_builder.append_value(
            block_data.header.parent_hash
            .to_string()
        );

        // -- SEAL ARRAYS --
        let number_array = number_builder.finish();
        let hash_array = hash_builder.finish();
        let parent_array = parent_builder.finish();

        // -- CREATE RECORD BATCH --
        let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(number_array),
            Arc::new(hash_array),
            Arc::new(parent_array),
        ],)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        
        Ok(format!("{:?}", batch))
    })
}

/// Asynchronously gets the latest block number from the Ethereum blockchain
#[pyfunction]
fn get_latest_block(py: Python<'_>, rpc_url: String) -> PyResult<&PyAny> {
    future_into_py(py, async move {
        // Parse the URL 
        let valid_url = Url::parse(&rpc_url) 
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid URL: {}", e)))?;
        
        // Build the Provider
        let provider = ProviderBuilder::new()
            .on_http(valid_url);

        // Fetch the block number
        let block_number = provider.get_block_number()
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to get block number: {}", e)))?;

        // Return Ok(block_number)
        Ok(block_number)
    })
}


#[pymodule]
fn eth_lake (_py: Python, m: &PyModule) -> PyResult<()>  {
    m.add_function(wrap_pyfunction!(get_latest_block, m)?)?;
    m.add_function(wrap_pyfunction!(fetch_block_arrow, m)?)?;
    Ok(())
}