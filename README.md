# StellarSQL

[![Build Status](https://travis-ci.org/stellarsql/StellarSQL.svg?branch=master)](https://travis-ci.org/stellarsql/StellarSQL)
[![codecov](https://codecov.io/gh/stellarsql/StellarSQL/branch/master/graph/badge.svg)](https://codecov.io/gh/stellarsql/StellarSQL)

(WIP) A minimal SQL DBMS written in Rust

- The document is [here](https://stellarsql.github.io/StellarSQL/stellar_sql/).
- There is a [slide](https://docs.google.com/presentation/d/1rSxFNpN5uzP1cF1olKTnyXgPdj_bcbLvSJhN5T5xn-U/edit?usp=sharing) introduce this project.
- There is a series of articles introducing about this project: [Let's build a DBMS](https://tigercosmos.xyz/lets-build-dbms/)

![logo](https://raw.githubusercontent.com/stellarsql/StellarSQL/master/logo/logo.png)

## Setup

Before you start, you need to have Rust(>=1.31) and Cargo.

```bash
curl https://sh.rustup.rs -sSf | sh
```

Then we could get the source code.

```bash
git clone https://github.com/tigercosmos/StellarSQL
cd StellarSQL
```

## Run

### Server

Open the first window and run server:

```bash
cargo run [port]
```

the default port is `23333`, and you can either modify `.env` or run by argument `[port]`.

### Client

Open the another window and run the client by `python client/client.py` and connect to the server:

command:

```sql
> create user <name> <key> # key is our feature, put any number for now
> set user <name> # second time log in
> create database <db_name> # first time create database
> use <db_name> # second time adopt the database
> <query> # now support simple sql
```

SQL query are not implement very well. A few simple command support for now:

- create database
- create table
  - type: int, float, double, varchar, char, url
- insert into
- select {fields} from {table} where {predicate}
  - not yet support join, only a table
  - predicate without NULL

The default `host` and `port` are `127.0.0.1` and `23333`

```shell
$ python client/client.py [host] [port]

Connect to 127.0.0.1:23333
== Welcome to StellarSQL Client! ==
StellarSQL> create user Tiger 123
Login OK!

StellarSQL> create database DB1
Query OK!

StellarSQL> create table t1 (a1 int, b1 int, c1 float);
Query OK!

StellarSQL> insert into t1 (a1, b1, c1) values (1, 2, 1.2), (2, 3, 4.5), (4, 1, 0.3);
Query OK!

StellarSQL> select a1, b1, c1 from t1 where a1 > 1;
{"fields":["a1","b1","c1"],"rows":[["2","3","4.5"],["4","1","0.3"]]}

StellarSQL> select a1, b1, c1 from t1 where a1 > 1 and c1 > 2;
{"fields":["a1","b1","c1"],"rows":[["2","3","4.5"]]}

StellarSQL> select a1, b1, c1 from t1 where not (not a1 < 2 and not (not b1 = 3 or c1 > 1.1));
{"fields":["a1","b1","c1"],"rows":[["1","2","1.2"],["2","3","4.5"],["4","1","0.3"]]}
```

## Build

```bash
cargo build
```

## Test

## Run all tests

```bash
cargo test
```

## Debug a test

Add the line at the beginning of the test function.

```rust
// init the logger for the test
env_logger::init();
```

Then run the command to see the debug information:

```sh
RUST_LOG=debug cargo test -- --nocapture {test_name}
```

## Pull Request

Install [rustfmt](https://github.com/rust-lang/rustfmt), and make sure you could pass:

```sh
cargo fmt --all -- --check
cargo build
cargo test
```

## Document

Build and open the document at localhost

```sh
cargo rustdoc --open -- --document-private-items
```

## License

MIT
