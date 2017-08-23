macro_rules! impl_setter {
  ($name:ident, $set_name:ident, $type:ty) => {
    #[cfg_attr(feature = "clippy", allow(float_cmp))]
    #[allow(dead_code)]
    pub fn $set_name(&mut self, value: $type) {
      if self.$name != value {
        self.set_modified();
        self.$name = value;
      }
    }
  };
}

macro_rules! impl_accessors {
  ($name:ident, $set_name:ident, $type:ty) => {
    impl_getter!($name, $type);
    impl_setter!($name, $set_name, $type);
  };
}

macro_rules! impl_record {
  () => {
    fn generation(&self) -> usize {
      self.generation
    }

    fn is_persisted(&self) -> bool {
      self.persisted
    }

    fn is_modified(&self) -> bool {
      self.modified
    }

    fn set_persisted(&mut self, value: bool) {
      self.persisted = value;
    }

    fn set_modified(&mut self) {
      self.modified = true;
      self.generation = 0;
    }

    fn set_not_modified(&mut self) {
      self.modified = false;
    }

    fn advance_generation(&mut self) {
      self.generation += 1;
    }
  };
}
