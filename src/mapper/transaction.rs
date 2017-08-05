use query::FindTransactionsResult;

#[derive(Clone)]
pub struct Transaction {
  persistent: bool,
  id_tx: u64,
  id_trunk: u64,
  id_branch: u64,
  da: i32,
  height: i32,
  solid: u8,
}

impl Transaction {
  pub fn placeholder(id_tx: u64, height: i32, solid: u8) -> Self {
    Self {
      persistent: false,
      id_trunk: 0,
      id_branch: 0,
      da: 1,
      id_tx,
      height,
      solid,
    }
  }

  pub fn id_tx(&self) -> u64 {
    self.id_tx
  }

  pub fn id_trunk(&self) -> u64 {
    self.id_trunk
  }

  pub fn id_branch(&self) -> u64 {
    self.id_branch
  }

  pub fn height(&self) -> i32 {
    self.height
  }

  pub fn solid(&self) -> u8 {
    self.solid
  }
}

impl<'a> From<&'a FindTransactionsResult> for Transaction {
  fn from(result: &FindTransactionsResult) -> Self {
    let FindTransactionsResult {
      id_tx,
      id_trunk,
      id_branch,
      da,
      height,
      solid,
    } = *result;
    Self {
      persistent: true,
      id_tx,
      id_trunk,
      id_branch,
      da,
      height,
      solid,
    }
  }
}
