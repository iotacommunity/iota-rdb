use iota_trytes::{TRITS_PER_TRYTE, trits_to_string};
use iota_trytes::num::int2trits;
use std::result;
use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};

pub fn milliseconds_since_epoch() -> result::Result<f64, SystemTimeError> {
  let duration = SystemTime::now().duration_since(UNIX_EPOCH)?;
  Ok(
    duration.as_secs() as f64 * 1000.0 +
      (duration.subsec_nanos() / 1_000_000) as f64,
  )
}

pub fn trits_string(number: isize, length: usize) -> Option<String> {
  let mut trits = vec![0; length * TRITS_PER_TRYTE];
  int2trits(number, &mut trits);
  trits_to_string(&trits)
}
