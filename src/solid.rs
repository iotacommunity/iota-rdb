#[derive(PartialEq, Copy, Clone, Debug)]
pub enum Solid {
  None,
  Trunk,
  Branch,
  Complete,
}

#[derive(Copy, Clone)]
pub enum Solidate {
  Trunk,
  Branch,
}

impl Solid {
  pub fn from_db(value: u8) -> Self {
    match value {
      0 => Solid::None,
      _ => Solid::Complete,
    }
  }

  pub fn into_db(self) -> u8 {
    match self {
      Solid::None | Solid::Trunk | Solid::Branch => 0,
      Solid::Complete => 1,
    }
  }

  pub fn is_complete(&self) -> bool {
    match *self {
      Solid::Complete => true,
      _ => false,
    }
  }

  pub fn solidate(&mut self, solidate: Solidate) -> bool {
    match solidate {
      Solidate::Trunk => match *self {
        Solid::None => {
          *self = Solid::Trunk;
          true
        }
        Solid::Branch => {
          *self = Solid::Complete;
          true
        }
        Solid::Trunk | Solid::Complete => false,
      },
      Solidate::Branch => match *self {
        Solid::None => {
          *self = Solid::Branch;
          true
        }
        Solid::Trunk => {
          *self = Solid::Complete;
          true
        }
        Solid::Branch | Solid::Complete => false,
      },
    }
  }
}
