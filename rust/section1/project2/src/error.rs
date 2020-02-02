use failure::Fail;

/// Error type for kvs.
#[derive(Fail, Debug)]
#[fail(display = "Error for kvs")]
pub enum KvsError {
    /// Unimplemented method.
    #[fail(display = "Unimplemented method")]
    Unimplemented,
}

/// Wrapper result type for kvs.
pub type Result<T> = std::result::Result<T, KvsError>;
