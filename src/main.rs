use anyhow::Result;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use itertools::Itertools;
use rand::{distributions::Uniform, prelude::*};
use rusqlite::{Connection, ErrorCode, OpenFlags, TransactionBehavior};
use serde::Serialize;
use std::{
    fs,
    ops::Add,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

/// Benchmarking SQLite
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to the SQLite file.
    #[arg(short, long)]
    path: PathBuf,

    /// Path to the output result file.
    #[arg(short, long)]
    output: PathBuf,

    /// Number of records to seed the into the table.
    #[arg(short, long, default_value_t = 1_000_000)]
    seed: usize,

    /// Number of concurrent threads to spawn.
    #[arg(short, long, num_args = 1.., value_delimiter = ' ', default_values_t = (1..=16).collect::<Vec<_>>())]
    threads: Vec<usize>,

    /// Scan operations to perform per transaction.
    #[arg(short, long, num_args = 1.., value_delimiter = ' ', default_values_t = vec![0, 10])]
    scans: Vec<usize>,

    /// Update operations to perform per transaction.
    #[arg(short, long, num_args = 1.., value_delimiter = ' ', default_values_t = vec![0, 1, 10])]
    updates: Vec<usize>,
}

struct Hexadecimal;
impl Distribution<char> for Hexadecimal {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> char {
        *b"0123456789ABCDEF".choose(rng).unwrap() as char
    }
}

const SCAN: &str = "SELECT * FROM tbl WHERE substr(c, 1, 16)>=? ORDER BY substr(c, 1, 16) LIMIT 10;";
const UPDATE: &str = "UPDATE tbl SET b=?, c=? WHERE a=?;";

#[derive(Debug, Serialize)]
struct Transactions {
    behavior: String,
    seed: usize,
    n_threads: usize,
    n_scans: usize,
    n_updates: usize,
    retries: usize,
    transactions: usize,
    tps: u128,
}

fn main() -> Result<()> {
    let args = Args::parse();

    if args.output.exists() {
        return Err(anyhow::anyhow!("file already exists {:?}", args.output));
    }

    // remove any state
    fs::remove_file(&args.path).ok();
    fs::remove_file(args.path.join("-shm")).ok();
    fs::remove_file(args.path.join("-wal")).ok();

    let iterations = args
        .threads
        .iter()
        .cartesian_product(args.scans)
        .cartesian_product(args.updates)
        .cartesian_product([
            TransactionBehavior::Deferred,
            TransactionBehavior::Immediate,
            TransactionBehavior::Concurrent,
        ])
        .map(|(((n_threads, n_scans), n_updates), trasaction_behavior)| (*n_threads, n_scans, n_updates, trasaction_behavior))
        .filter(|(_, n_scans, n_updates, _)| !(*n_scans == 0 && *n_updates == 0))
        .collect::<Vec<_>>();

    // seed database
    seed(&args.path, args.seed)?;

    let pb = ProgressBar::new(iterations.len() as u64).with_style(ProgressStyle::with_template("{wide_bar} {pos}/{len} {eta_precise}")?);
    pb.inc(0);

    let mut results = Vec::with_capacity(iterations.len());

    for (n_threads, n_scans, n_updates, trasaction_behavior) in iterations {
        results.push(begin(&args.path, args.seed, n_threads, n_scans, n_updates, trasaction_behavior)?);
        pb.inc(1);
    }

    pb.finish();

    fs::write(args.output, serde_json::to_string_pretty(&results)?)?;

    // remove any state
    fs::remove_file(&args.path).ok();
    fs::remove_file(args.path.join("-shm")).ok();
    fs::remove_file(args.path.join("-wal")).ok();

    Ok(())
}

fn seed(path: &Path, rows: usize) -> Result<()> {
    let conn = Connection::open(path)?;
    conn.execute_batch(&format!(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA mmap_size = 1000000000;
        PRAGMA synchronous = off;
        PRAGMA journal_size_limit = 16777216;

        CREATE TABLE tbl(
            a INTEGER PRIMARY KEY,
            b BLOB(200),
            c CHAR(64)
        );

        -- https://www.sqlite.org/series.html
        WITH RECURSIVE generate_series(value) AS (
            SELECT 0
            UNION ALL
            SELECT value+1 FROM generate_series
            WHERE value < {rows}
        )
        INSERT INTO tbl
        SELECT value, randomblob(200), hex(randomblob(32))
        FROM generate_series;

        CREATE INDEX tbl_i1 ON tbl(substr(c, 1, 16));
        CREATE INDEX tbl_i2 ON tbl(substr(c, 2, 16));
        "
    ))?;

    Ok(())
}

fn begin(
    path: &Path,
    seed: usize,
    n_threads: usize,
    n_scans: usize,
    n_updates: usize,
    trasaction_behavior: TransactionBehavior,
) -> Result<Transactions> {
    let transactions = Arc::new(AtomicUsize::new(0));
    let retries = Arc::new(AtomicUsize::new(0));
    (0..n_threads)
        .map(|thread_id| {
            let path = path.to_path_buf();
            let transactions = transactions.clone();
            let retries = retries.clone();

            std::thread::spawn(move || {
                let between_ids = Uniform::from(0..1_000_000);
                let mut rng: StdRng = SeedableRng::seed_from_u64(thread_id as u64);
                let mut conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
                conn.busy_timeout(Duration::from_millis(5000))?;

                let finish_time = Instant::now().add(Duration::from_secs(30));
                while Instant::now() <= finish_time {
                    let scans = (0..n_scans)
                        .map(|_| (&mut rng).sample_iter(&Hexadecimal).take(16).map(char::from).collect::<String>())
                        .collect::<Vec<_>>();
                    let updates: Vec<([u8; 200], String, i32)> = (0..n_updates)
                        .map(|_| {
                            let mut bytes = [0; 200];
                            rng.fill_bytes(&mut bytes);
                            (
                                bytes,
                                (&mut rng).sample_iter(&Hexadecimal).take(64).map(char::from).collect::<String>(),
                                between_ids.sample(&mut rng),
                            )
                        })
                        .collect::<Vec<_>>();

                    loop {
                        let mut transaction = || {
                            let txn = conn.transaction_with_behavior(trasaction_behavior)?;

                            if !scans.is_empty() {
                                let mut scan = txn.prepare_cached(SCAN)?;
                                for random_hex in &scans {
                                    _ = scan.query_map([random_hex], |row| row.get::<_, i32>(0))?;
                                }
                            }

                            if !updates.is_empty() {
                                let mut update = txn.prepare_cached(UPDATE)?;
                                for (random_bytes, random_hex, random_id) in &updates {
                                    update.execute((random_bytes, random_hex, random_id))?;
                                }
                            }

                            txn.commit()
                        };

                        match transaction() {
                            Err(rusqlite::Error::SqliteFailure(err, _)) if err.code == ErrorCode::DatabaseBusy => {
                                retries.fetch_add(1, Ordering::Relaxed);
                                continue;
                            }
                            Ok(_) => {
                                transactions.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                            err => unimplemented!("{err:?}"),
                        }
                    }
                }

                anyhow::Ok(())
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .for_each(|thread| thread.join().expect("should not fail").expect("should not fail"));

    Ok(Transactions {
        behavior: match trasaction_behavior {
            TransactionBehavior::Deferred => "DEFERRED",
            TransactionBehavior::Immediate => "IMMEDIATE",
            TransactionBehavior::Exclusive => "EXCLUSIVE",
            TransactionBehavior::Concurrent => "CONCURRENT",
            _ => unreachable!(),
        }
        .to_string(),
        seed,
        n_threads,
        n_scans,
        n_updates,
        retries: retries.load(Ordering::Relaxed),
        transactions: transactions.load(Ordering::Relaxed),
        tps: Duration::from_secs(1).as_nanos() / Duration::from_secs(30).div_f32(transactions.load(Ordering::Relaxed) as f32).as_nanos(),
    })
}
