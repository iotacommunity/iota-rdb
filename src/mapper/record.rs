use super::{Error, Result};
use mysql;

pub trait Record: Sized {
  const SELECT_QUERY: &'static str;
  const SELECT_WHERE_ID: &'static str;
  const INSERT_QUERY: &'static str;
  const UPDATE_QUERY: &'static str;

  fn from_row(row: &mut mysql::Row) -> Result<Self>;

  fn find(conn: &mut mysql::Conn, id: u64) -> Result<Self> {
    Ok(conn
      .first_exec(
        format!("{}, {}", Self::SELECT_QUERY, Self::SELECT_WHERE_ID),
        (id,),
      )?
      .ok_or(Error::RecordNotFound)
      .and_then(|mut row| Self::from_row(&mut row))?)
  }

  fn insert(&mut self, conn: &mut mysql::Conn) -> Result<()> {
    conn.prep_exec(Self::INSERT_QUERY, self.to_params())?;
    self.set_persistent(true);
    self.set_modified(false);
    Ok(())
  }

  fn update(&mut self, conn: &mut mysql::Conn) -> Result<()> {
    conn.prep_exec(Self::UPDATE_QUERY, self.to_params())?;
    self.set_modified(false);
    Ok(())
  }

  fn take_column<T>(row: &mut mysql::Row, column: &str, default: T) -> Result<T>
  where
    T: mysql::value::FromValue,
  {
    match row.take_opt(column) {
      Some(value) => Ok(value?),
      None => Ok(default),
    }
  }

  fn to_params(&self) -> Vec<(String, mysql::Value)>;

  fn is_persistent(&self) -> bool;

  fn is_modified(&self) -> bool;

  fn is_locked(&self) -> bool;

  fn set_persistent(&mut self, value: bool);

  fn set_modified(&mut self, value: bool);

  fn lock(&mut self);

  fn unlock(&mut self);
}
