use std::fmt::Display;

pub enum Role {
    Master,
    Slave,
}

impl Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Master => write!(f, "master"),
            Self::Slave => write!(f, "slave"),
        }
    }
}

pub struct Info {
    pub role: Role,
}
