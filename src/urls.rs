use serde::Deserialize;
use serde_json::Value;
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
    result: ResultData,
}

struct Client {
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
    pub fn latest_query_result(self, query_id: &str) {
        let url = format!("https://api.dune.com/api/v1/query/{}/results", query_id);
        let request = ureq::get(&url).header("X-Dune-API-Key", self.api_key.clone());

        match request.call() {
            Ok(mut response) => {
                println!("Response: {:?}", response);
                // get body
                let body = response.body_mut().read_to_string().unwrap();
                let data: ExecutionInfo = serde_json::from_str(&body).unwrap();
                dbg!(&data);

                // Save data to file
                self.save(query_id, &body);

                // loop through rows
                for column_name in data.result.metadata.column_names {
                    let value = data.result.rows[0].get(column_name.clone()).unwrap();
                    let typ = data.result.metadata.column_types[0].clone();
                    println!("Column Name: {:?}", column_name);
                    println!("value: {:?}, type: {:?}", value, typ);
                    match typ.as_str() {
                        "double" => {
                            let value = value.as_f64().unwrap();
                            println!("True Value: {:?}", value);
                        }
                        "string" => {
                            let value = value.as_str().unwrap();
                            println!("True Value: {:?}", value);
                        }
                        _ => {
                            println!("Unknown type: {:?}", typ);
                        }
                    }
                }
                for row in data.result.rows {
                    println!("Row: {:?}", row);
                }
            }
            Err(e) => panic!("Error: {:?}", e),
        }
    }

    /// Get API key
    pub fn get_api_key() -> Result<String, std::env::VarError> {
        std::env::var("DUNE_API_KEY")
    }
}

#[cfg(test)]
mod tests {
    #[ignore]
    #[test]
    fn test_get_api_key() {
        let api_key = super::Client::get_api_key().unwrap();
    }

    //
    //     Object {
    //       "txs": Number(0.07152323),
    //       "unique_senders_addresses": Number(1.167168),
    //     },
    //     ],
    //     metadata: Metadata {
    //     column_names: [
    //     "txs",
    //     "unique_senders_addresses",
    //     ],
    //     column_types: [
    //     "double",
    //     "double",
    //     ],
    #[test]
    fn test_latest_query_result() {
        let api_key = super::Client::get_api_key().unwrap();
        let client = super::Client::new_with_key(api_key);
        client.latest_query_result("3557348");
    }

    #[test]
    fn test_save() {
        let query = "3557348";
        let results = "latest_query_result";
        let api_key = super::Client::get_api_key().unwrap();
        let client = super::Client::new_with_key(api_key);
        client.save(query, results);
    }
}
