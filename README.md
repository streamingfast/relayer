# dfuse Relayer

[![reference](https://img.shields.io/badge/godoc-reference-5272B4.svg?style=flat-square)](https://pkg.go.dev/github.com/dfuse-io/relayer)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The relayer process fans out and propagates blocks from instrumented
blockchain nodes, down to services, serving as a redundant and
highly-available access to streaming block & transaction data.
It is part of **[dfuse](https://github.com/dfuse-io/dfuse)**.

## Installation & Usage

See the different protocol-specific `dfuse` binaries at https://github.com/dfuse-io/dfuse#protocols

Current implementations:

* [**dfuse for EOSIO**](https://github.com/dfuse-io/dfuse-eosio)
* **dfuse for Ethereum**, soon to be open sourced


## Contributing

**Issues and PR in this repo related strictly to the relayer functionalities**

Report any protocol-specific issues in their
[respective repositories](https://github.com/dfuse-io/dfuse#protocols)

**Please first refer to the general
[dfuse contribution guide](https://github.com/dfuse-io/dfuse/blob/master/CONTRIBUTING.md)**,
if you wish to contribute to this code base.


## License

[Apache 2.0](LICENSE)
