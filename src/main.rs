use std::{
    collections::BTreeSet,
    fs::OpenOptions,
    hash::Hash,
    io::{BufReader, BufWriter, Read},
    time::SystemTime,
};

use anyhow::{Context, Result};
use clap::Parser;
use datafusion::{
    arrow::util::pretty::print_batches,
    prelude::{CsvReadOptions, SessionContext},
};
use serde::{Deserialize, Serialize};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, num_args = 1..)]
    input_files: Vec<String>,
    #[arg(short, long)]
    query: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct QueryLog {
    timestamp: u64,
    query: String,
}

impl QueryLog {
    pub fn try_new(query: &str) -> Result<Self> {
        Ok(Self {
            timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_secs(),
            query: query.to_string(),
        })
    }
}

impl Ord for QueryLog {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timestamp.cmp(&other.timestamp)
    }
}

impl PartialOrd for QueryLog {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.timestamp.partial_cmp(&other.timestamp)
    }
}

impl Hash for QueryLog {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.query.hash(state)
    }
}

struct QueryLogRepository {
    logs: BTreeSet<QueryLog>,
}

const LOG_PATH: &str = "test.json";

impl QueryLogRepository {
    pub fn try_new() -> Result<Self> {
        let log_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(LOG_PATH)
            .with_context(|| "Log file open error")?;

        let mut contents = String::new();

        let mut reader = BufReader::new(log_file);
        reader.read_to_string(&mut contents)?;

        let logs = if contents.is_empty() {
            // ログがまだ存在しない場合は新しいBTreeSetを作成
            BTreeSet::new()
        } else {
            // 既存のログがある場合はそれをデシリアライズ
            serde_json::from_str(&contents)?
        };

        Ok(Self { logs })
    }

    pub fn add(&mut self, query: &str) -> Result<()> {
        let log = QueryLog::try_new(query)?;
        self.logs.replace(log);

        if self.logs.len() > 100 {
            let first_key = self.logs.iter().next().unwrap().clone();
            self.logs.remove(&first_key);
        }

        let writer = BufWriter::new(std::fs::File::create(LOG_PATH)?);
        serde_json::to_writer_pretty(writer, &self.logs)?;

        Ok(())
    }
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
