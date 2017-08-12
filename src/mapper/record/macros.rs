macro_rules! impl_setter {
  ($name:ident, $set_name:ident, $type:ty, in $restricted:path) => {
    #[cfg_attr(feature = "clippy", allow(float_cmp))]
    #[allow(dead_code)]
    pub(in $restricted) fn $set_name(&mut self, value: $type) {
      impl_setter!(body, $name, self, value);
    }
  };

  ($name:ident, $set_name:ident, $type:ty) => {
    #[cfg_attr(feature = "clippy", allow(float_cmp))]
    #[allow(dead_code)]
    pub fn $set_name(&mut self, value: $type) {
      impl_setter!(body, $name, self, value);
    }
  };

  (body, $name:ident, $self:ident, $value:ident) => {
    if $self.$name != $value {
      $self.modified = true;
      $self.$name = $value;
    }
  }
}

macro_rules! impl_accessors {
  ($name:ident, $set_name:ident, $type:ty) => {
    impl_getter!($name, $type);
    impl_setter!($name, $set_name, $type);
  };
}

macro_rules! impl_record {
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
