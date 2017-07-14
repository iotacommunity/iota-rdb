use std::result;
use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};

pub fn milliseconds_since_epoch() -> result::Result<f64, SystemTimeError> {
  let duration = SystemTime::now().duration_since(UNIX_EPOCH)?;
  Ok(
    duration.as_secs() as f64 * 1000.0 +
      (duration.subsec_nanos() / 1_000_000) as f64,
  )
}
