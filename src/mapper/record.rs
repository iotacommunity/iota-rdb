pub trait Record {
  fn is_locked(&self) -> bool;

  fn lock(&mut self);

  fn unlock(&mut self);
}
