macro_rules! impl_getter {
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
