use pyo3::prelude::*;
use pyo3_asyncio::tokio::future_into_py;
use tokio::time;
use alloy::providers::{Provider, ProviderBuilder};
use url::Url;

/// Sums two numbers and returns the result as a string
#[pyfunction]
    fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
        Ok((a + b).to_string())
    }

/// Asynchronously sleeps for a given number of seconds
#[pyfunction]
fn sleep_for (py: Python<'_>, seconds: u64) -> PyResult<&PyAny> {
    future_into_py(py, async move {
        time::sleep(std::time::Duration::from_secs(seconds)).await;
        Ok(format!("Slept for {} seconds", seconds))
    })
}

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
    m.add_function(wrap_pyfunction!(sleep_for, m)?)?;
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_function(wrap_pyfunction!(get_latest_block, m)?)?;

    Ok(())
}