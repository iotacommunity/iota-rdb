use super::Result;
use event;
use mapper::{AddressMapper, AddressRecord, BundleMapper, BundleRecord, Mapper,
             Record, TransactionMapper, TransactionRecord};
use mysql;
use solid::{Solid, Solidate};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::SystemTime;
use utils::SystemTimeUtils;
use worker::{ApproveJob, CalculateJob, SolidateJob};

pub const TAG_LENGTH: usize = 27;

type UnwrappedTransactions<'a> = Option<
  (
    (u64, &'a Mutex<TransactionRecord>),
    Option<(u64, &'a Mutex<TransactionRecord>)>,
    &'a Mutex<TransactionRecord>,
  ),
>;

#[derive(Debug)]
pub struct TransactionMessage {
  hash: String,
  address_hash: String,
  value: i64,
  tag: String,
  timestamp: f64,
  current_index: i32,
  last_index: i32,
  bundle_hash: String,
  trunk_hash: String,
  branch_hash: String,
  arrival: f64,
  is_mst: bool,
  solid: Solid,
}

impl TransactionMessage {
  pub fn parse(
    source: &str,
    milestone_address: &str,
    milestone_start_index: &str,
  ) -> Result<Self> {
    let chunks: Vec<&str> = source.split(' ').collect();
    let hash = chunks[1].to_owned();
    let address_hash = chunks[2].to_owned();
    let value = chunks[3].parse()?;
    let tag = chunks[4][..TAG_LENGTH].to_owned();
    let timestamp = chunks[5].parse()?;
    let current_index = chunks[6].parse()?;
    let last_index = chunks[7].parse()?;
    let bundle_hash = chunks[8].to_owned();
    let trunk_hash = chunks[9].to_owned();
    let branch_hash = chunks[10].to_owned();
    let arrival = normalize_timestamp(chunks[11].parse()?);
    let is_mst = address_hash == milestone_address;
    let solid = if is_mst && tag == milestone_start_index {
      Solid::Complete
    } else {
      Solid::None
    };
    Ok(Self {
      hash,
      address_hash,
      value,
      tag,
      timestamp,
      current_index,
      last_index,
      bundle_hash,
      trunk_hash,
      branch_hash,
      arrival,
      is_mst,
      solid,
    })
  }

  pub fn hash(&self) -> &str {
    &self.hash
  }

  pub fn perform(
    &self,
    conn: &mut mysql::Conn,
    transaction_mapper: &TransactionMapper,
    address_mapper: &AddressMapper,
    bundle_mapper: &BundleMapper,
    null_hash: &str,
  ) -> Result<
    (
      Option<ApproveJob>,
      Option<SolidateJob>,
      Option<CalculateJob>,
    ),
  > {
    let (mut approve_data, mut solidate_data, mut calculate_data) =
      (None, None, None);
    let txs = transaction_mapper
      .fetch_many(conn, vec![&self.trunk_hash, &self.branch_hash, &self.hash])?;
    let (_, address) =
      address_mapper.fetch_by_hash(conn, &self.address_hash, |id_address| {
        AddressRecord::new(id_address, self.address_hash.to_owned())
      })?;
    let (id_bundle, bundle) =
      bundle_mapper.fetch_by_hash(conn, &self.bundle_hash, |id_bundle| {
        Ok(BundleRecord::new(id_bundle, self.bundle_hash.to_owned()))
      })?;
    let txs = self.unwrap_transactions(null_hash, &txs);
    if let Some(((id_trunk, trunk_tx), branch_tx, current_tx)) = txs {
      let trunk_index = transaction_mapper.trunk_index(id_trunk).unwrap();
      let branch_index = branch_tx
        .map(|(id_branch, _)| id_branch)
        .unwrap_or(id_trunk);
      let branch_index = transaction_mapper.branch_index(branch_index).unwrap();
      let bundle_index = bundle_mapper.transaction_index(id_bundle).unwrap();
      debug!("Mutex lock");
      let mut trunk_index = trunk_index.lock().unwrap();
      debug!("Mutex lock/acquire");
      let mut branch_index = branch_index.lock().unwrap();
      debug!("Mutex lock/acquire");
      let mut bundle_index = bundle_index.lock().unwrap();
      debug!("Mutex lock/acquire");
      let mut trunk_tx = trunk_tx.lock().unwrap();
      debug!("Mutex lock/acquire");
      let mut branch_tx = branch_tx
        .as_ref()
        .map(|&(_, branch_tx)| branch_tx.lock().unwrap());
      debug!("Mutex lock/acquire");
      let mut current_tx = current_tx.lock().unwrap();
      debug!("Mutex lock/acquire");
      let mut address = address.lock().unwrap();
      debug!("Mutex lock/acquire");
      let mut bundle = bundle.lock().unwrap();
      debug!("Mutex acquire");
      if !current_tx.is_persisted() {
        let timestamp = SystemTime::milliseconds_since_epoch()?;
        process_parent(conn, null_hash, &mut trunk_tx)?;
        current_tx.set_id_trunk(trunk_tx.id_tx(), &mut trunk_index);
        if let Some(ref mut branch_tx) = branch_tx {
          process_parent(conn, null_hash, branch_tx)?;
          current_tx.set_id_branch(branch_tx.id_tx(), &mut branch_index);
        } else {
          current_tx.set_id_branch(trunk_tx.id_tx(), &mut branch_index);
        }
        current_tx.set_id_address(address.id_address());
        current_tx.set_id_bundle(bundle.id_bundle(), &mut bundle_index);
        current_tx.set_tag(self.tag.to_owned());
        current_tx.set_value(self.value);
        current_tx.set_timestamp(self.timestamp);
        current_tx.set_arrival(self.arrival);
        current_tx.set_current_idx(self.current_index);
        current_tx.set_last_idx(self.last_index);
        current_tx.set_is_mst(bundle.is_mst() || self.is_mst);
        current_tx.set_mst_a(bundle.is_mst() || self.is_mst);
        if self.is_mst {
          bundle.set_is_mst(true);
        }
        self.set_solid(&mut current_tx, &trunk_tx, &branch_tx);
        self.set_height(&mut current_tx, &trunk_tx);
        self.insert_events(conn, &current_tx, timestamp)?;
        self.set_approve_data(&mut approve_data, &current_tx);
        self.set_solidate_data(&mut solidate_data, &current_tx);
        self.set_calculate_data(&mut calculate_data, &current_tx);
        if !address.is_persisted() {
          address.insert(conn)?;
        }
        if !bundle.is_persisted() {
          bundle.insert(conn)?;
        }
        current_tx.insert(conn)?;
      }
    }
    Ok((approve_data, solidate_data, calculate_data))
  }

  fn unwrap_transactions<'a>(
    &self,
    null_hash: &str,
    transactions: &'a [(u64, String, Arc<Mutex<TransactionRecord>>)],
  ) -> UnwrappedTransactions<'a> {
    if self.hash == null_hash || self.hash == self.branch_hash {
      return None;
    }
    let (mut current_tx, mut trunk_tx, mut branch_tx) = (None, None, None);
    for &(id_tx, ref hash, ref transaction) in transactions {
      if hash == &self.hash {
        current_tx = Some(&**transaction);
      } else if hash == &self.trunk_hash {
        trunk_tx = Some((id_tx, &**transaction));
      } else if hash == &self.branch_hash {
        branch_tx = Some((id_tx, &**transaction));
      }
    }
    current_tx.and_then(|current_tx| {
      trunk_tx.map(|trunk_tx| (trunk_tx, branch_tx, current_tx))
    })
  }

  fn set_solid(
    &self,
    current_tx: &mut TransactionRecord,
    trunk_tx: &TransactionRecord,
    branch_tx: &Option<MutexGuard<TransactionRecord>>,
  ) {
    let mut solid = self.solid;
    let mut is_complete = trunk_tx.solid().is_complete();
    if is_complete {
      solid.solidate(Solidate::Trunk);
    }
    if let Some(ref branch_tx) = *branch_tx {
      is_complete = branch_tx.solid().is_complete();
    }
    if is_complete {
      solid.solidate(Solidate::Branch);
    }
    current_tx.set_solid(solid);
  }

  fn set_height(
    &self,
    current_tx: &mut TransactionRecord,
    trunk_tx: &TransactionRecord,
  ) {
    if !self.solid.is_complete() && trunk_tx.solid().is_complete() {
      current_tx.set_height(trunk_tx.height() + 1);
    } else {
      current_tx.set_height(0);
    }
  }

  fn insert_events(
    &self,
    conn: &mut mysql::Conn,
    current_tx: &TransactionRecord,
    timestamp: f64,
  ) -> Result<()> {
    event::new_transaction_received(conn, timestamp)?;
    if !current_tx.solid().is_complete() {
      event::unsolid_transaction(conn, timestamp)?;
    }
    if self.is_mst {
      event::milestone_received(conn, timestamp)?;
    }
    Ok(())
  }

  fn set_approve_data(
    &self,
    approve_data: &mut Option<ApproveJob>,
    current_tx: &TransactionRecord,
  ) {
    *approve_data = if self.is_mst {
      current_tx.id_trunk().and_then(|id_trunk| {
        current_tx.id_branch().and_then(|id_branch| {
          current_tx.id_bundle().map(|id_bundle| {
            ApproveJob::milestone(
              id_bundle,
              id_trunk,
              id_branch,
              current_tx.mst_timestamp(),
            )
          })
        })
      })
    } else if current_tx.mst_a() {
      current_tx.id_trunk().and_then(|id_trunk| {
        current_tx.id_branch().map(|id_branch| {
          ApproveJob::front(id_trunk, id_branch, current_tx.mst_timestamp())
        })
      })
    } else {
      Some(ApproveJob::reverse(current_tx.id_tx()))
    };
  }

  fn set_solidate_data(
    &self,
    solidate_data: &mut Option<SolidateJob>,
    current_tx: &TransactionRecord,
  ) {
    if current_tx.solid().is_complete() {
      *solidate_data =
        Some(SolidateJob::new(current_tx.id_tx(), current_tx.height()));
    }
  }

  fn set_calculate_data(
    &self,
    calculate_data: &mut Option<CalculateJob>,
    current_tx: &TransactionRecord,
  ) {
    *calculate_data = Some(CalculateJob::new(current_tx.id_tx()));
  }
}

fn process_parent(
  conn: &mut mysql::Conn,
  null_hash: &str,
  tx: &mut TransactionRecord,
) -> Result<()> {
  tx.direct_approve();
  if !tx.is_persisted() && tx.hash() == null_hash {
    tx.set_solid(Solid::Complete);
    tx.insert(conn)?;
  }
  Ok(())
}

fn normalize_timestamp(timestamp: f64) -> f64 {
  const THRESHOLD: f64 = 1_262_304_000_000.0; // 01.01.2010 in milliseconds
  if timestamp > THRESHOLD {
    (timestamp / 1000.0).floor()
  } else {
    timestamp
  }
}
