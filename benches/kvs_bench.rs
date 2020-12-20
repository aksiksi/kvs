use criterion::{criterion_group, criterion_main, Criterion};
use rand::Rng;
use rand::distributions::{Alphanumeric, Distribution, Uniform};
use rand::seq::SliceRandom;
use tempfile::TempDir;

use kvs::{KvStore, KvsEngine, SledKvsEngine};

const NUM_VALUES: usize = 100;

fn kvs_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("kvs");
    let group = group.sample_size(10);

    // Create a temporary directory for the bench
    let dir = TempDir::new().unwrap();
    let path = dir.path();

    let mut kvs = KvStore::open(path).unwrap();

    // Generate 100 keys and values of random length in [1, 100000] bytes.
    let mut rng = rand::thread_rng();
    let between = Uniform::from(1..100000);
    let key_lengths = (0..NUM_VALUES)
        .map(|_| between.sample(&mut rng))
        .collect::<Vec<_>>();
    let value_lengths = (0..NUM_VALUES)
        .map(|_| between.sample(&mut rng))
        .collect::<Vec<_>>();
    let mut pairs = Vec::with_capacity(NUM_VALUES);

    for (key_length, value_length) in key_lengths.iter().zip(value_lengths.iter()) {
        let key: String = (0..*key_length)
            .map(|_| char::from(rng.sample(Alphanumeric)))
            .collect();
        let value: String = (0..*value_length)
            .map(|_| char::from(rng.sample(Alphanumeric)))
            .collect();
        pairs.push((key, value));
    }

    group.bench_function("kvs_write 100", |b| {
        b.iter(|| {
            for (key, value) in pairs.iter() {
                kvs.set(key.clone(), value.clone()).unwrap();
            }
        });
    });

    let random_keys = (0..1000).map(|_| {
        let random_key = pairs.choose(&mut rng).unwrap().0.clone();
        random_key
    }).collect::<Vec<_>>();

    group.bench_function("kvs_read 1000", |b| {
        b.iter(|| {
            for key in random_keys.iter() {
                kvs.get(key.clone()).unwrap();
            }
        });
    });
}

fn sled_bench(c: &mut Criterion) {
    // Create a temporary directory for the bench
    let dir = TempDir::new().unwrap();
    let path = dir.path();

    let mut sled = SledKvsEngine::open(path).unwrap();

    // Generate 100 keys and values of random length in [1, 100000] bytes.
    let mut rng = rand::thread_rng();
    let between = Uniform::from(1..100000);
    let key_lengths = (0..NUM_VALUES)
        .map(|_| between.sample(&mut rng))
        .collect::<Vec<_>>();
    let value_lengths = (0..NUM_VALUES)
        .map(|_| between.sample(&mut rng))
        .collect::<Vec<_>>();
    let mut pairs = Vec::with_capacity(NUM_VALUES);

    for (key_length, value_length) in key_lengths.iter().zip(value_lengths.iter()) {
        let key: String = (0..*key_length)
            .map(|_| char::from(rng.sample(Alphanumeric)))
            .collect();
        let value: String = (0..*value_length)
            .map(|_| char::from(rng.sample(Alphanumeric)))
            .collect();
        pairs.push((key, value));
    }

    c.bench_function("sled_write 100", |b| {
        b.iter(|| {
            for (key, value) in pairs.iter() {
                sled.set(key.clone(), value.clone()).unwrap();
            }
        });
    });

    let random_keys = (0..1000).map(|_| {
        let random_key = pairs.choose(&mut rng).unwrap().0.clone();
        random_key
    }).collect::<Vec<_>>();

    c.bench_function("sled_read 1000", |b| {
        b.iter(|| {
            for key in random_keys.iter() {
                sled.get(key.clone()).unwrap();
            }
        });
    });
}

criterion_group!(benches, kvs_bench, sled_bench);
criterion_main!(benches);
