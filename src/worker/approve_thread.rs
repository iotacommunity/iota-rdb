use super::Result;
use event;
use mapper::{BundleMapper, Mapper, Record, TransactionMapper,
             TransactionRecord};
use mysql;
use std::collections::{HashSet, VecDeque};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Instant, SystemTime};
use utils::{DurationUtils, MysqlConnUtils, SystemTimeUtils};

#[derive(Debug)]
pub enum ApproveJob {
  Reverse(ReverseApproveJob),
  Front(FrontApproveJob),
  Milestone(MilestoneApproveJob),
}

#[derive(Debug)]
pub struct ReverseApproveJob {
  id: u64,
}

#[derive(Debug)]
pub struct FrontApproveJob {
  id_trunk: u64,
  id_branch: u64,
  mst_timestamp: f64,
}

#[derive(Debug)]
pub struct MilestoneApproveJob {
  id_bundle: u64,
  id_trunk: u64,
  id_branch: u64,
  mst_timestamp: f64,
}

pub struct ApproveThread<'a> {
  pub approve_rx: mpsc::Receiver<ApproveJob>,
  pub mysql_uri: &'a str,
  pub retry_interval: u64,
  pub transaction_mapper: Arc<TransactionMapper>,
  pub bundle_mapper: Arc<BundleMapper>,
}

impl<'a> ApproveThread<'a> {
  pub fn spawn(self) {
    let Self {
      approve_rx,
      mysql_uri,
      retry_interval,
      transaction_mapper,
      bundle_mapper,
    } = self;
    let mut conn = mysql::Conn::new_retry(mysql_uri, retry_interval);
    thread::spawn(move || {
      let transaction_mapper = &*transaction_mapper;
      let bundle_mapper = &*bundle_mapper;
      loop {
        let job = approve_rx.recv().expect("Thread communication failure");
        let duration = Instant::now();
        let result = job.perform(&mut conn, transaction_mapper, bundle_mapper);
        let duration = duration.elapsed().as_milliseconds();
        match result {
          Ok(()) => {
            info!("{:.3}ms {:?}", duration, job);
          }
          Err(err) => {
            error!("{:.3}ms {}", duration, err);
          }
        }
      }
    });
  }
}

impl ApproveJob {
  pub fn reverse(id: u64) -> Self {
    ApproveJob::Reverse(ReverseApproveJob::new(id))
  }

  pub fn front(id_trunk: u64, id_branch: u64, mst_timestamp: f64) -> Self {
    ApproveJob::Front(FrontApproveJob::new(id_trunk, id_branch, mst_timestamp))
  }

  pub fn milestone(
    id_bundle: u64,
    id_trunk: u64,
    id_branch: u64,
    mst_timestamp: f64,
  ) -> Self {
    ApproveJob::Milestone(MilestoneApproveJob::new(
      id_bundle,
      id_trunk,
      id_branch,
      mst_timestamp,
    ))
  }

  fn perform(
    &self,
    conn: &mut mysql::Conn,
    transaction_mapper: &TransactionMapper,
    bundle_mapper: &BundleMapper,
  ) -> Result<()> {
    match *self {
      ApproveJob::Reverse(ref reverse) => {
        reverse.perform(conn, transaction_mapper)
      }
      ApproveJob::Front(ref front) => front.perform(conn, transaction_mapper),
      ApproveJob::Milestone(ref milestone) => {
        milestone.perform(conn, transaction_mapper, bundle_mapper)
      }
    }
  }
}

impl ReverseApproveJob {
  fn new(id: u64) -> Self {
    Self { id }
  }

  fn perform(
    &self,
    conn: &mut mysql::Conn,
    transaction_mapper: &TransactionMapper,
  ) -> Result<()> {
    let mut mst_timestamp = None;
    if let Some(index) = transaction_mapper.trunk_index(self.id) {
      if let Some(ref index) =
        *transaction_mapper.fetch_trunk(conn, self.id, &index)?
      {
        mst_timestamp = approved_child(conn, transaction_mapper, index)?;
      }
    }
    if mst_timestamp.is_none() {
      if let Some(index) = transaction_mapper.branch_index(self.id) {
        if let Some(ref index) =
          *transaction_mapper.fetch_branch(conn, self.id, &index)?
        {
          mst_timestamp = approved_child(conn, transaction_mapper, index)?;
        }
      }
    }
    if let Some(mst_timestamp) = mst_timestamp {
      let (id_trunk, id_branch) = {
        let tid = thread::current().id();
        let transaction = transaction_mapper.fetch(conn, self.id)?;
        debug!("Mutex check at line {} {:?}", line!(), tid);
        let mut transaction = transaction.lock().unwrap();
        debug!("Mutex check at line {} {:?}", line!(), tid);
        approve(&mut transaction, mst_timestamp)?;
        (transaction.id_trunk(), transaction.id_branch())
      };
      if let (Some(id_trunk), Some(id_branch)) = (id_trunk, id_branch) {
        FrontApproveJob::new(id_trunk, id_branch, mst_timestamp)
          .perform(conn, transaction_mapper)?;
      }
    }
    Ok(())
  }
}

impl FrontApproveJob {
  fn new(id_trunk: u64, id_branch: u64, mst_timestamp: f64) -> Self {
    Self {
      id_trunk,
      id_branch,
      mst_timestamp,
    }
  }

  fn perform(
    &self,
    conn: &mut mysql::Conn,
    transaction_mapper: &TransactionMapper,
  ) -> Result<()> {
    let (timestamp, mut counter) = (SystemTime::milliseconds_since_epoch()?, 0);
    let (mut nodes, mut visited) = (VecDeque::new(), HashSet::new());
    nodes.push_front(self.id_trunk);
    if self.id_branch != self.id_trunk {
      nodes.push_front(self.id_branch);
    }
    while let Some(id) = nodes.pop_back() {
      if !visited.insert(id) {
        continue;
      }
      let transaction = transaction_mapper.fetch(conn, id)?;
      let tid = thread::current().id();
      debug!("Mutex check at line {} {:?}", line!(), tid);
      let mut transaction = transaction.lock().unwrap();
      debug!("Mutex check at line {} {:?}", line!(), tid);
      if transaction.mst_a() || !transaction.is_persisted() {
        continue;
      }
      if let Some(id_trunk) = transaction.id_trunk() {
        nodes.push_front(id_trunk);
      }
      if let Some(id_branch) = transaction.id_branch() {
        nodes.push_front(id_branch);
      }
      approve(&mut transaction, self.mst_timestamp)?;
      counter += 1;
    }
    if counter > 0 {
      event::subtangle_confirmation(conn, timestamp, counter)?;
    }
    Ok(())
  }
}

impl MilestoneApproveJob {
  fn new(
    id_bundle: u64,
    id_trunk: u64,
    id_branch: u64,
    mst_timestamp: f64,
  ) -> Self {
    Self {
      id_bundle,
      id_trunk,
      id_branch,
      mst_timestamp,
    }
  }

  fn perform(
    &self,
    conn: &mut mysql::Conn,
    transaction_mapper: &TransactionMapper,
    bundle_mapper: &BundleMapper,
  ) -> Result<()> {
    let mut ids = vec![(self.id_trunk, self.id_branch)];
    if let Some(index) = bundle_mapper.transaction_index(self.id_bundle) {
      if let Some(ref index) =
        *transaction_mapper.fetch_bundle(conn, self.id_bundle, &index)?
      {
        for &id in index {
          let record = transaction_mapper.fetch(conn, id)?;
          let tid = thread::current().id();
          debug!("Mutex check at line {} {:?}", line!(), tid);
          let mut record = record.lock().unwrap();
          debug!("Mutex check at line {} {:?}", line!(), tid);
          record.set_is_mst(true);
          if !record.mst_a() {
            if let (Some(id_trunk), Some(id_branch)) =
              (record.id_trunk(), record.id_branch())
            {
              ids.push((id_trunk, id_branch));
            }
          }
        }
      }
    }
    for (id_trunk, id_branch) in ids {
      FrontApproveJob::new(id_trunk, id_branch, self.mst_timestamp)
        .perform(conn, transaction_mapper)?;
    }
    Ok(())
  }
}

fn approved_child(
  conn: &mut mysql::Conn,
  transaction_mapper: &TransactionMapper,
  index: &[u64],
) -> Result<Option<f64>> {
  for &id in index {
    let tid = thread::current().id();
    let record = transaction_mapper.fetch(conn, id)?;
    debug!("Mutex check at line {} {:?}", line!(), tid);
    let record = record.lock().unwrap();
    debug!("Mutex check at line {} {:?}", line!(), tid);
    if record.mst_a() {
      return Ok(Some(record.mst_timestamp()));
    }
  }
  Ok(None)
}

fn approve(
  transaction: &mut TransactionRecord,
  mst_timestamp: f64,
) -> Result<()> {
  let timestamp = transaction.timestamp();
  transaction.set_conftime(mst_timestamp - timestamp);
  transaction.set_mst_a(true);
  Ok(())
}
