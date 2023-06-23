use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    fs::OpenOptions,
    hash::Hash,
    io::{BufReader, BufWriter, Read},
    time::SystemTime,
};

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
