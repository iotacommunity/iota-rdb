# README

## Table of Contents
* [Usage](#usage)
* [Building](#building)
* [Contributing](#contributing)
* [License](#license)
  + [Contribution](#contribution)

## Usage

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

Make sure [Docker][docker] and [Docker Compose][docker-compose] are installed.

Edit `docker-compose.yml` file for configuration.

Run the service with the following command:

```sh
$ docker-compose up --build
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

## License

Licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
   http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or
   http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.

[rust]: https://www.rust-lang.org/
[rustfmt]: https://github.com/rust-lang-nursery/rustfmt
[clippy]: https://github.com/rust-lang-nursery/rust-clippy
[zmq]: https://github.com/zeromq/libzmq
[docker]: https://www.docker.com/community-edition#/download
[docker-compose]: https://docs.docker.com/compose/install/
