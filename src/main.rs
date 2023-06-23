mod query_log;

use anyhow::{Context, Result};
use clap::Parser;
use datafusion::{
    arrow::util::pretty::print_batches,
    prelude::{CsvReadOptions, SessionContext},
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, num_args = 1..)]
    input_files: Vec<String>,
    #[arg(short, long)]
    query: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    //let mut log_repository = QueryLogRepository::try_new()?;

    let ctx = SessionContext::new();

    for (i, file_path) in args.input_files.iter().enumerate() {
        ctx.register_csv(&format!("i{}", i), file_path, CsvReadOptions::default())
            .await?;
    }

    let query = if let Some(query) = args.query {
        query
    } else {
        todo!()
    };

    let df = ctx.sql(&query).await?;

    let results = df.collect().await?;
    print_batches(&results)?;

    // log_repository.add(&query).with_context(|| "Log File write error")??;
    Ok(())
}
