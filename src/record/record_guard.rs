use super::Record;
use std::ops::{Deref, DerefMut, Drop};

pub struct RecordGuard<'a, T: Record + 'a> {
  record: &'a mut T,
}

impl<'a, T: Record> Deref for RecordGuard<'a, T> {
  type Target = T;

  fn deref(&self) -> &T {
    self.record
  }
}

impl<'a, T: Record> DerefMut for RecordGuard<'a, T> {
  fn deref_mut(&mut self) -> &mut T {
    self.record
  }
}

impl<'a, T: Record> Drop for RecordGuard<'a, T> {
  fn drop(&mut self) {
    self.record.unlock();
  }
}

impl<'a, T: Record> RecordGuard<'a, T> {
  pub fn new(record: &'a mut T) -> Self {
    Self { record }
  }
}
