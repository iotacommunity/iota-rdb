use iota_curl_cpu;
use iota_sign;
use iota_trytes;
use mysql;
use std::thread;
use std::time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH};

pub trait SystemTimeUtils {
  fn milliseconds_since_epoch() -> Result<f64, SystemTimeError>;
}

pub trait DurationUtils {
  fn as_milliseconds(&self) -> f64;
}

pub trait MysqlConnUtils {
  fn new_retry(uri: &str, retry_interval: u64) -> Self;
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

impl MysqlConnUtils for mysql::Conn {
  fn new_retry(uri: &str, retry_interval: u64) -> Self {
    const CREATE_DB: &str = include_str!("../db/create-db.sql");
    let retry_interval = Duration::from_millis(retry_interval);
    let root_uri = format!("{}/?prefer_socket=false", uri);
    let iota_uri = format!("{}/iota?prefer_socket=false", uri);
    loop {
      match mysql::Conn::new(&iota_uri) {
        Ok(conn) => return conn,
        Err(mysql::Error::MySqlError(ref err)) if err.code == 1049 => {
          if let Ok(mut conn) = mysql::Conn::new(&root_uri) {
            if let Err(err) = conn.query(CREATE_DB) {
              warn!("MySQL create db failure: {}. Retrying...", err);
              thread::sleep(retry_interval);
            }
          }
        }
        Err(err) => {
          warn!("MySQL connection failure: {}. Retrying...", err);
          thread::sleep(retry_interval);
        }
      }
    }
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
