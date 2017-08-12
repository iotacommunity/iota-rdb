use iota_curl_cpu;
use iota_sign;
use iota_trytes;
use std::time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH};

pub trait SystemTimeUtils {
  fn milliseconds_since_epoch() -> Result<f64, SystemTimeError>;
}

pub trait DurationUtils {
  fn as_milliseconds(&self) -> f64;
}

impl SystemTimeUtils for SystemTime {
  fn milliseconds_since_epoch() -> Result<f64, SystemTimeError> {
    let duration = Self::now().duration_since(UNIX_EPOCH)?;
    Ok(duration.as_milliseconds())
  }
}

impl DurationUtils for Duration {
  fn as_milliseconds(&self) -> f64 {
    self.as_secs() as f64 * 1000.0 + self.subsec_nanos() as f64 * 1e-6
  }
}

pub fn trits_string(number: isize, length: usize) -> Option<String> {
  let mut trits = vec![0; length * iota_trytes::TRITS_PER_TRYTE];
  iota_trytes::num::int2trits(number, &mut trits);
  iota_trytes::trits_to_string(&trits)
}

pub fn trits_checksum(source: &str) -> Option<String> {
  let mut checksum = [0; iota_sign::CHECKSUM_LEN];
  let mut curl = iota_curl_cpu::CpuCurl::default();
  let trits: Vec<_> = source
    .chars()
    .flat_map(iota_trytes::char_to_trits)
    .cloned()
    .collect();
  iota_sign::trits_checksum(&trits, &mut checksum, &mut curl);
  iota_trytes::trits_to_string(&checksum)
}
