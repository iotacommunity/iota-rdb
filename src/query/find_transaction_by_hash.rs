pub struct FindTransactionByHash {}
// select_transactions_by_hash: pool.prepare(
//   r#"
//     SELECT
//       id_tx, id_trunk, id_branch, height, solid
//     FROM tx
//     WHERE hash = :hash
//   "#,
// )?,
//
// pub fn select_transaction_by_hash(
//   &mut self,
//   hash: &str,
// ) -> Result<Option<ReferencedTransaction>> {
//   match self
//     .select_transactions_by_hash
//     .first_exec(params!{"hash" => hash})? {
//     Some(mut row) => Ok(Some(ReferencedTransaction {
//       id_tx: row.take_opt("id_tx").ok_or(Error::ColumnNotFound)?,
//       id_trunk: row.take_opt("id_trunk").ok_or(Error::ColumnNotFound)?,
//       id_branch: row.take_opt("id_branch").ok_or(Error::ColumnNotFound)?,
//       height: row.take_opt("height").ok_or(Error::ColumnNotFound)?,
//       solid: row.take_opt("solid").ok_or(Error::ColumnNotFound)?,
//     })),
//     None => Ok(None),
//   }
// }
