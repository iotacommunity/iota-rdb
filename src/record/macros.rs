macro_rules! define_setter {
  ($name:ident, $set_name:ident, $type:ty) => {
    #[allow(dead_code, float_cmp)]
    pub fn $set_name(&mut self, value: $type) {
      if self.$name != value {
        self.modified = true;
        self.$name = value;
      }
    }
  };
}

macro_rules! define_accessors {
  ($name:ident, $set_name:ident, $type:ty) => {
    define_getter!($name, $type);
    define_setter!($name, $set_name, $type);
  };
}

macro_rules! define_record {
  () => {
    fn is_persisted(&self) -> bool {
      self.persisted
    }

    fn is_modified(&self) -> bool {
      self.modified
    }

    fn set_persisted(&mut self, value: bool) {
      self.persisted = value;
    }

    fn set_modified(&mut self, value: bool) {
      self.modified = value;
    }
  };
}
