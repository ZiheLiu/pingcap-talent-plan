use std::collections::HashMap;
use tempfile::TempDir;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, ParameterizedBenchmark};

use rand::distributions;
use rand::{Rng, SeedableRng};

use kvs::{KvStore, KvsEngine, SledKvsEngine};

const GET_CMDS_TOTAL: usize = 1000;

fn gen_set_data(commands_total: usize) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    pairs.reserve(commands_total);

    let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(2020);
    for _ in 0..commands_total {
        let len = rng.gen_range(1, 100001);
        let key: String = (&mut rng)
            .sample_iter(&distributions::Alphanumeric)
            .take(len)
            .collect();

        let len = rng.gen_range(1, 100001);
        let value: String = (&mut rng)
            .sample_iter(&distributions::Alphanumeric)
            .take(len)
            .collect();

        pairs.push((key, value));
    }

    pairs
}

fn gen_get_data(command_total: usize, pairs: &Vec<(String, String)>) -> Vec<(String, String)> {
    // Distinct pairs by key.
    let mut map = HashMap::new();
    pairs.iter().for_each(|(k, v)| {
        map.insert(k.clone(), v.clone());
    });
    let pairs: Vec<(String, String)> = map.into_iter().map(|(k, v)| (k, v)).collect();

    let mut get_pairs = Vec::new();
    get_pairs.reserve(command_total);

    let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(2020);
    for _ in 0..command_total {
        let i = rng.gen_range(0, pairs.len());
        get_pairs.push(pairs[i].clone());
    }

    get_pairs
}

fn write_bench(c: &mut Criterion) {
    let group = ParameterizedBenchmark::new(
        "kvs",
        |b, &set_cmds_total| {
            let pairs = gen_set_data(set_cmds_total);

            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let kvs_engine = KvStore::open(temp_dir.path()).unwrap();
                    // move temp_dir to routine to avoid to drop it.
                    (pairs.clone(), kvs_engine, temp_dir)
                },
                |(mut pairs, mut kvs_engine, _)| {
                    while let Some((k, v)) = pairs.pop() {
                        kvs_engine.set(k, v).unwrap();
                    }
                },
                BatchSize::SmallInput,
            );
        },
        vec![20, 40, 60, 80, 100],
    )
    .with_function("sled", |b, &set_cmds_total| {
        let pairs = gen_set_data(set_cmds_total);

        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let sled_engine = SledKvsEngine::open(temp_dir.path()).unwrap();
                // move temp_dir to routine to avoid to drop it.
                (pairs.clone(), sled_engine, temp_dir)
            },
            |(mut pairs, mut sled_engine, _)| {
                while let Some((k, v)) = pairs.pop() {
                    sled_engine.set(k, v).unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });

    c.bench("write_bench", group.sample_size(10));
}

fn read_bench(c: &mut Criterion) {
    let group = ParameterizedBenchmark::new(
        "kvs",
        |b, &set_cmds_total| {
            let mut set_pairs = gen_set_data(set_cmds_total);
            let get_pairs = gen_get_data(GET_CMDS_TOTAL, &set_pairs);

            let temp_dir = TempDir::new().unwrap();
            let mut kvs_engine = KvStore::open(temp_dir.path()).unwrap();
            // write `set_cmds_total` pairs to store.
            while let Some((k, v)) = set_pairs.pop() {
                kvs_engine.set(k, v).unwrap();
            }

            b.iter_batched(
                || get_pairs.clone(),
                |mut get_pairs| {
                    while let Some((k, v)) = get_pairs.pop() {
                        assert_eq!(kvs_engine.get(k).unwrap(), Some(v));
                    }
                },
                BatchSize::SmallInput,
            );
        },
        vec![20, 40, 60, 80, 100],
    )
    .with_function("sled", |b, &set_cmds_total| {
        let mut set_pairs = gen_set_data(set_cmds_total);
        let get_pairs = gen_get_data(GET_CMDS_TOTAL, &set_pairs);

        let temp_dir = TempDir::new().unwrap();
        let mut sled_engine = SledKvsEngine::open(temp_dir.path()).unwrap();
        // write `set_cmds_total` pairs to store.
        while let Some((k, v)) = set_pairs.pop() {
            sled_engine.set(k, v).unwrap();
        }

        b.iter_batched(
            || get_pairs.clone(),
            |mut get_pairs| {
                while let Some((k, v)) = get_pairs.pop() {
                    assert_eq!(sled_engine.get(k).unwrap(), Some(v));
                }
            },
            BatchSize::SmallInput,
        );
    });

    c.bench("read_bench", group.sample_size(10));
}

criterion_group!(benches, write_bench, read_bench);
criterion_main!(benches);
