#[macro_use]
mod macros;
mod transaction_record;
mod address_record;
mod bundle_record;

pub use self::address_record::AddressRecord;
pub use self::bundle_record::BundleRecord;
pub use self::transaction_record::TransactionRecord;

use super::{Error, Result};
use mysql;

pub trait Record: Sized {
  const SELECT_QUERY: &'static str;
  const SELECT_WHERE_ID: &'static str;
  const SELECT_WHERE_HASH: &'static str;
  const INSERT_QUERY: &'static str;
  const UPDATE_QUERY: &'static str;

  fn from_row(row: &mut mysql::Row) -> Result<Self>;

  fn find_by_id(conn: &mut mysql::Conn, id: u64) -> Result<Self> {
    Ok(conn
      .first_exec(
        format!("{}, {}", Self::SELECT_QUERY, Self::SELECT_WHERE_ID),
        (id,),
      )?
      .ok_or(Error::RecordNotFound)
      .and_then(|mut row| Self::from_row(&mut row))?)
  }

  fn find_by_hash(conn: &mut mysql::Conn, hash: &str) -> Result<Option<Self>> {
    match conn.first_exec(
      format!("{} {}", Self::SELECT_QUERY, Self::SELECT_WHERE_HASH),
      (hash,),
    )? {
      Some(ref mut row) => Ok(Some(Self::from_row(row)?)),
      None => Ok(None),
    }
  }

  fn insert(&mut self, conn: &mut mysql::Conn) -> Result<()> {
    conn.prep_exec(Self::INSERT_QUERY, self.to_params())?;
    self.set_persisted(true);
    self.set_modified(false);
    Ok(())
  }

  fn update(&mut self, conn: &mut mysql::Conn) -> Result<()> {
    conn.prep_exec(Self::UPDATE_QUERY, self.to_params())?;
    self.set_modified(false);
    Ok(())
  }

  fn to_params(&self) -> Vec<(String, mysql::Value)>;

  fn is_persisted(&self) -> bool;

  fn is_modified(&self) -> bool;

  fn set_persisted(&mut self, value: bool);

  fn set_modified(&mut self, value: bool);

  fn id(&self) -> u64;

  fn hash(&self) -> &str;
}
