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
