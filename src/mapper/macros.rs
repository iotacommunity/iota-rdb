macro_rules! define_setter {
  ($name:ident, $setter:ident, $type:ty) => {
    #[allow(dead_code, float_cmp)]
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

macro_rules! define_record {
  () => {
    fn is_persistent(&self) -> bool {
      self.persistent
    }

    fn is_modified(&self) -> bool {
      self.modified
    }

    fn is_locked(&self) -> bool {
      self.locked
    }

    fn set_persistent(&mut self, value: bool) {
      self.persistent = value;
    }

    fn set_modified(&mut self, value: bool) {
      self.modified = value;
    }

    fn lock(&mut self) {
      self.locked = true;
    }

    fn unlock(&mut self) {
      self.locked = false;
    }
  };
}
