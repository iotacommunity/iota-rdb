# README

## Table of Contents
* [Building](#building)
* [Usage](#usage)

## Building

We are using the *nightly* channel of [Rust][rust]. To install it, run the
following command:

```sh
$ curl https://sh.rustup.rs -sSf | sh
```

You may need to relogin to update your environment variables.

Currently, the project requires ZeroMQ 3.2 or newer. For example, on recent
Debian-based distributions, you can use the following command to get the
prerequisite headers and library installed:

```sh
$ apt-get install libzmq3-dev pkg-config
```

Once you installed all required dependencies, run the following command in the
project root to build executable:

```sh
$ cargo build --release
```

The binary will be located at `target/release/iota-rdb`.

## Usage

To run the program it is required to pass ZeroMQ and MySQL addresses as follows:

```sh
$ iota-rdb \
    --zmq 'tcp://127.0.0.1:5556' \
    --mysql 'mysql://root:password@127.0.0.1:3306/iota' \
    --write-threads 4 \
    --approve-threads 4
```

You can view available options by running with `--help` argument:

```sh
$ iota-rdb --help
```

[rust]: https://www.rust-lang.org/
