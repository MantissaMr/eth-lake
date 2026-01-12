use pyo3::prelude::*;
use pyo3_asyncio::tokio::future_into_py;
use tokio::time;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::eth::{BlockNumberOrTag};
use url::Url;


#[pyfunction]
fn fetch_block_arrow(py: Python<'_>, rpc_url: String, block_number: u64) -> PyResult<&PyAny> {
    future_into_py(py, async move {
        // Parse the URL
        let valid_url = Url::parse(&rpc_url)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid URL: {}", e)))?;

        // Build the Provider
        let provider = ProviderBuilder::new()
            .on_http(valid_url);

        // Convert the u64 into the Alloy Enum
        let alloy_block_number = BlockNumberOrTag::Number(block_number);

        // Fetch the FULL block (true = include full transaction details)
        let block_data = provider.get_block_by_number(alloy_block_number, true)
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to get block data: {}", e)))?
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Block not found".to_string()))?;
        
        // Return the block hash as a string to prove we got it, deal with Arrow conversion later
        let hash_string = block_data.header.hash.unwrap_or_default().to_string();        
        Ok(hash_string)
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

/// Asynchronously sleeps for a given number of seconds
#[pyfunction]
fn sleep_for (py: Python<'_>, seconds: u64) -> PyResult<&PyAny> {
    future_into_py(py, async move {
        time::sleep(std::time::Duration::from_secs(seconds)).await;
        Ok(format!("Slept for {} seconds", seconds))
    })
}


#[pymodule]
fn eth_lake (_py: Python, m: &PyModule) -> PyResult<()>  { 
    m.add_function(wrap_pyfunction!(sleep_for, m)?)?;
    m.add_function(wrap_pyfunction!(get_latest_block, m)?)?;
    m.add_function(wrap_pyfunction!(fetch_block_arrow, m)?)?;
    Ok(())
}