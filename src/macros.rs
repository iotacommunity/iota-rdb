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

macro_rules! define_setter {
  ($name:ident, $setter:ident, $type:ty) => {
    #[allow(dead_code)]
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

macro_rules! eprint {
  ($str:expr) => {
    {
      use ::std::io::Write;
      write!(::std::io::stderr(), $str).unwrap();
    }
  };

  ($($arg:tt)*) => {
    {
      use ::std::io::Write;
      write!(::std::io::stderr(), $($arg)*).unwrap();
    }
  };
}

macro_rules! eprintln {
  () => {
    eprint!("\n");
  };

  ($fmt:expr) => {
    eprint!(concat!($fmt, "\n"));
  };

  ($fmt:expr, $($arg:tt)*) => {
    eprint!(concat!($fmt, "\n"), $($arg)*);
  };
}
