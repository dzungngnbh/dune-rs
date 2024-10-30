#[derive(thiserror::Error, Debug)]
pub enum DuneError {
    #[error("Failed")]
    Failed,
}
