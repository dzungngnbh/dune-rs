use crate::errors::DuneError;

use anyhow::{bail, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use std::fs;

#[derive(Deserialize, Debug)]
struct ResultData {
    rows: Vec<Value>,
    metadata: Metadata,
}

#[derive(Deserialize, Debug)]
struct Metadata {
    column_names: Vec<String>,
    column_types: Vec<String>,
    row_count: usize,
    result_set_bytes: usize,
    total_row_count: usize,
    total_result_set_bytes: usize,
    datapoint_count: usize,
    pending_time_millis: i64,
    execution_time_millis: i64,
}

#[derive(Deserialize, Debug)]
struct ExecutionInfo {
    execution_id: String,
    query_id: u64,
    is_execution_finished: bool,
    state: String,
    submitted_at: String,
    expires_at: String,
    execution_started_at: String,
    execution_ended_at: String,
    result: Option<ResultData>,
}

///
/// Dune client
///
pub struct Client {
    api_key: String,

    // generate from api_key
    id: String,
}

impl Client {
    pub fn new() -> Self {
        let api_key = Self::get_api_key().unwrap();
        let id = faster_hex::hex_string(api_key.as_bytes());
        Self { api_key, id }
    }

    pub fn new_with_key(api_key: &str) -> Self {
        let id = faster_hex::hex_string(api_key.as_bytes());
        Self {
            api_key: api_key.to_string(),
            id,
        }
    }

    ///
    /// Save data to cache
    ///
    pub fn save(&self, query: &str, results: &str) {
        // create ~/.duners/cache
        let duners_dir = dirs::home_dir()
            .ok_or("Failed to get home directory")
            .unwrap()
            .join(".duners");
        let cache_dir = duners_dir.join("cache").join(&self.id);
        fs::create_dir_all(&cache_dir).unwrap();

        // save to file with name query with content results
        let file = cache_dir.join(query);
        fs::write(file, results).unwrap();
    }

    ///
    /// Get latest query result
    /// https://docs.dune.com/api-reference/executions/endpoint/get-query-result
    ///
    pub fn latest_query_result(&self, query_id: &str) -> Result<Vec<Value>> {
        let url = format!("https://api.dune.com/api/v1/query/{}/results", query_id);
        let request = ureq::get(&url).header("X-Dune-API-Key", self.api_key.clone());

        match request.call() {
            Ok(mut response) => {
                let body = response.body_mut().read_to_string().unwrap();
                let data: ExecutionInfo = serde_json::from_str(&body).unwrap();

                match data.state.as_str() {
                    "QUERY_STATE_COMPLETED" => {
                        self.save(query_id, &body);
                        let rows = data.result.unwrap().rows;
                        Ok(rows)
                    }
                    _ => bail!(DuneError::Failed),
                }
            }
            Err(e) => Err(anyhow::anyhow!("Error: {:?}", e)),
        }
    }

    ///
    /// Execute query
    ///
    pub fn execute(self, query_id: &str) -> Result<String> {
        let url = format!("https://api.dune.com/api/v1/query/{}/execute", query_id);
        let request = ureq::post(&url)
            .header("X-Dune-API-Key", self.api_key.clone())
            .send_json(json!({
                "query_id": query_id,
                "api_key": self.api_key.clone(),
            }));

        match request {
            Ok(mut response) => {
                // get body
                let body = response.body_mut().read_to_string()?;
                let data: ExecutionInfo = serde_json::from_str(&body)?;

                match data.state.as_str() {
                    "QUERY_STATE_COMPLETED" => {
                        // Save data to file
                        self.save(query_id, &body);
                        Ok(body)
                    }
                    _ => bail!(DuneError::Failed),
                }
            }
            Err(e) => Err(anyhow::anyhow!("Error: {:?}", e)),
        }
    }

    /// Get API key
    pub fn get_api_key() -> Result<String> {
        dotenvy::dotenv().ok();
        Ok(std::env::var("DUNE_API_KEY")?)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_latest_query_result() {
        let api_key = super::Client::get_api_key().unwrap();
        let client = super::Client::new_with_key(&api_key);
        match client.latest_query_result("3557348") {
            Ok(body) => {
                println!("{:?}", body);
            }
            Err(e) => println!("Error: {:?}", e),
        }

        match client.latest_query_result("123") {
            Ok(body) => {
                println!("{:?}", body);
            }
            Err(e) => println!("Error: {:?}", e),
        }
    }

    #[test]
    fn test_execute() {
        let api_key = super::Client::get_api_key().unwrap();
        let client = super::Client::new_with_key(&api_key);
        match client.execute("3557348") {
            Ok(body) => {
                println!("{:?}", body);
            }
            Err(e) => println!("Error: {:?}", e),
        }
    }

    #[test]
    fn test_save() {
        let query = "3557348";
        let results = "latest_query_result";
        let api_key = super::Client::get_api_key().unwrap();
        let client = super::Client::new_with_key(&api_key);
        client.save(query, results);
    }
}
