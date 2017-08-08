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
