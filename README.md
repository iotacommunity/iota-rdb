# README

## Table of Contents
* [Checking Out](#checking-out)
* [Building](#building)
* [Usage](#usage)
* [Contributing](#contributing)

## Checking Out

Since we are using git submodules system, additional steps to work with the
repository are required. To correctly clone the repository you should add
`--recursive` argument:

```sh
$ git clone --recursive https://github.com/iotacommunity/iota-rdb
```

To correctly pull changes from the upstream, run the following commands:

```sh
$ git pull
$ git submodule update --init --recursive
```

## Building

The project is written in [Rust][rust] programming language. To install it, run
the following command:

```sh
$ curl https://sh.rustup.rs -sSf | sh
```

You may need to relogin to update your environment variables.

We are using nightly channel of rust. If you haven't checked it in the previous
step, install it with the following command:

```sh
$ rustup install nightly
```

Currently, the project requires [ZeroMQ][zmq] 3.2 or newer. For example, on
recent Debian-based distributions, you can use the following command to get the
prerequisite headers and library installed:

```sh
$ apt-get install libzmq3-dev pkg-config
```

Once you installed all required dependencies, run the following command in the
project root to build executable:

```sh
$ rustup run nightly cargo build --release
```

The binary will be located at `target/release/iota-rdb`.

## Usage

To run the program it is required to pass ZeroMQ and MySQL addresses as follows:

```sh
$ iota-rdb \
    --zmq 'tcp://127.0.0.1:5556' \
    --mysql 'mysql://root:password@127.0.0.1:3306/iota' \
    --write-threads 4 \
    --approve-threads 4 \
    --solidate-threads 4
```

You can view available options by running with `--help` argument:

```sh
$ iota-rdb --help
```

## Contributing

Please check the following steps before contributing to the project:

1. Follow syntax guidelines.

We are using [rustfmt][rustfmt] tool to automatically style the code. You can
install it with the following command:

```sh
$ rustup run nightly cargo install rustfmt-nightly
```

To format the source codes run the following command:

```sh
$ rustup run nightly cargo fmt
```

2. Check that the linter produces no warnings.

We are using [clippy][clippy] rust linter as a development dependency. To check
the lints run the following commands:

```sh
$ rustup run nightly cargo build --features "clippy"
```

The output should not contain warnings.

[rust]: https://www.rust-lang.org/
[rustfmt]: https://github.com/rust-lang-nursery/rustfmt
[clippy]: https://github.com/rust-lang-nursery/rust-clippy
[zmq]: https://github.com/zeromq/libzmq