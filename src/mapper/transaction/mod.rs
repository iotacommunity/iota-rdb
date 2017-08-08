mod select;
mod insert;
mod update;

use mapper::Record;
use mysql;
use std::collections::HashMap;

#[derive(Clone)]
pub struct Transaction {
  locked: bool,
  persistent: bool,
  modified: bool,
  hash: String,
  id_tx: u64,
  id_trunk: u64,
  id_branch: u64,
  id_address: u64,
  id_bundle: u64,
  tag: String,
  value: i64,
  timestamp: i64,
  current_idx: i32,
  last_idx: i32,
  da: i32,
  height: i32,
  is_mst: bool,
  mst_a: bool,
  solid: u8,
}

macro_rules! define_getter {
  ($name:ident, &$type:ty) => {
    #[allow(dead_code)]
    pub fn $name(&self) -> &$type {
      &self.$name
    }
  };

  ($name:ident, $type:ty) => {
    #[allow(dead_code)]
    pub fn $name(&self) -> $type {
      self.$name
    }
  };
}

macro_rules! define_setter {
  ($name:ident, $setter:ident, $type:ty) => {
    #[allow(dead_code)]
    pub fn $setter(&mut self, value: $type) {
      if self.$name != value {
        self.modified = true;
        self.$name = value;
      }
    }
  };
}

macro_rules! define_accessors {
  ($name:ident, $setter:ident, $type:ty) => {
    define_getter!($name, $type);
    define_setter!($name, $setter, $type);
  };
}

impl Record for Transaction {
  fn is_locked(&self) -> bool {
    self.locked
  }

  fn lock(&mut self) {
    self.locked = true;
  }

  fn unlock(&mut self) {
    self.locked = false;
  }
}

impl Transaction {
  pub fn placeholder(hash: String, id_tx: u64, solid: u8) -> Self {
    Self {
      locked: false,
      persistent: false,
      modified: true,
      hash,
      id_tx,
      id_trunk: 0,
      id_branch: 0,
      id_address: 0,
      id_bundle: 0,
      tag: String::from(""),
      value: 0,
      timestamp: 0,
      current_idx: 0,
      last_idx: 0,
      da: 0,
      height: 0,
      is_mst: false,
      mst_a: false,
      solid,
    }
  }

  define_getter!(hash, &str);
  define_getter!(id_tx, u64);
  define_accessors!(id_trunk, set_id_trunk, u64);
  define_accessors!(id_branch, set_id_branch, u64);
  define_accessors!(id_address, set_id_address, u64);
  define_accessors!(id_bundle, set_id_bundle, u64);
  define_setter!(tag, set_tag, String);
  define_accessors!(value, set_value, i64);
  define_accessors!(timestamp, set_timestamp, i64);
  define_accessors!(current_idx, set_current_idx, i32);
  define_accessors!(last_idx, set_last_idx, i32);
  define_accessors!(height, set_height, i32);
  define_accessors!(is_mst, set_is_mst, bool);
  define_accessors!(mst_a, set_mst_a, bool);
  define_accessors!(solid, set_solid, u8);

  pub fn is_persistent(&self) -> bool {
    self.persistent
  }

  pub fn is_modified(&self) -> bool {
    self.modified
  }

  pub fn direct_approve(&mut self) {
    self.modified = true;
    self.da += 1;
  }

  pub fn approve(&mut self) {
    if !self.mst_a {
      self.modified = true;
      self.mst_a = true;
    }
  }

  pub fn to_params(&self) -> Vec<(String, mysql::Value)> {
    params!{
      "id_tx" => self.id_tx,
      "id_trunk" => self.id_trunk,
      "id_branch" => self.id_branch,
      "id_address" => self.id_address,
      "id_bundle" => self.id_bundle,
      "tag" => self.tag.clone(),
      "value" => self.value,
      "timestamp" => self.timestamp,
      "current_idx" => self.current_idx,
      "last_idx" => self.last_idx,
      "da" => self.da,
      "height" => self.height,
      "is_mst" => self.is_mst,
      "mst_a" => self.mst_a,
      "solid" => self.solid,
    }
  }

  pub fn store(
    &self,
    records: &mut HashMap<u64, Transaction>,
    hashes: &mut HashMap<String, u64>,
  ) {
    records.insert(self.id_tx(), self.clone());
    hashes.insert(self.hash().to_owned(), self.id_tx());
  }
}
