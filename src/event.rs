use mysql;

const QUERY: &str = r#"
  INSERT INTO txload (
    event, count, timestamp
  ) VALUES (
    :event, :count, :timestamp
  )
"#;

pub fn new_transaction_received(
  conn: &mut mysql::Conn,
  timestamp: f64,
) -> mysql::Result<()> {
  conn.prep_exec(
    QUERY,
    params!{
      "event" => "NTX",
      "count" => 1,
      "timestamp" => timestamp,
    },
  )?;
  Ok(())
}

pub fn milestone_received(
  conn: &mut mysql::Conn,
  timestamp: f64,
) -> mysql::Result<()> {
  conn.prep_exec(
    QUERY,
    params!{
      "event" => "MST",
      "count" => 1,
      "timestamp" => timestamp,
    },
  )?;
  Ok(())
}

pub fn subtangle_confirmation(
  conn: &mut mysql::Conn,
  timestamp: f64,
  count: i32,
) -> mysql::Result<()> {
  conn.prep_exec(
    QUERY,
    params!{
      "event" => "CNF",
      "count" => count,
      "timestamp" => timestamp,
    },
  )?;
  Ok(())
}

pub fn unsolid_transaction(
  conn: &mut mysql::Conn,
  timestamp: f64,
) -> mysql::Result<()> {
  conn.prep_exec(
    QUERY,
    params!{
      "event" => "UNS",
      "count" => 1,
      "timestamp" => timestamp,
    },
  )?;
  Ok(())
}

pub fn subtangle_solidation(
  conn: &mut mysql::Conn,
  timestamp: f64,
  count: i32,
) -> mysql::Result<()> {
  conn.prep_exec(
    QUERY,
    params!{
      "event" => "SOL",
      "count" => count,
      "timestamp" => timestamp,
    },
  )?;
  Ok(())
}
