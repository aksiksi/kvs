use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tempfile::TempDir;

use kvs::{KvStore, KvsEngine};

fn kvs_bench(c: &mut Criterion) {
    let d1 = TempDir::new().unwrap();
    let d2 = TempDir::new().unwrap();

    let mut s1 = KvStore::open(d1.path()).unwrap();
    let mut kvs_set = |n| {
        for i in 0..n {
            let k = format!("key{}", i);
            let v = format!("val{}", i);
            s1.set(k, v).unwrap();
        }
    };

    let mut s2 = KvStore::open(d2.path()).unwrap();

    for i in 0..1000 {
        let k = format!("key{}", i);
        let v = format!("val{}", i);
        s2.set(k, v).unwrap();
    }

    let mut kvs_get = |n, m: i32| {
        for i in 0..n {
            let k = format!("key{}", i % m);
            s2.get(k).unwrap();
        }
    };

    c.bench_function("kvs_set 1000", |b| b.iter(|| black_box(kvs_set(1000))));
    c.bench_function("kvs_get 1000", |b| {
        b.iter(|| black_box(kvs_get(1000, 1000)))
    });
}

criterion_group!(benches, kvs_bench);
criterion_main!(benches);
