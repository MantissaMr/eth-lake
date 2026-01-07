use pyo3::prelude::*;
use pyo3_asyncio::tokio::future_into_py;
use tokio::time;

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


#[pymodule]
fn eth_lake (_py: Python, m: &PyModule) -> PyResult<()>  { 
    m.add_function(wrap_pyfunction!(sleep_for, m)?)?;
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;

    Ok(())
}