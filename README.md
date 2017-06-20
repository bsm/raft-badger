raft-badger
===========

[![Build Status](https://travis-ci.org/bsm/raft-badger.png?branch=master)](https://travis-ci.org/bsm/raft-badger)
[![GoDoc](https://godoc.org/github.com/bsm/raft-badger?status.png)](http://godoc.org/github.com/bsm/raft-badger)
[![Go Report Card](https://goreportcard.com/badge/github.com/bsm/raft-badger)](https://goreportcard.com/report/github.com/bsm/raft-badger)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)


The package exports the `BadgerStore` which is an implementation of both
a `LogStore` and `StableStore` on top of [Badger](https://github.com/dgraph-io/badger),
an embeddable, persistent, simple and fast key-value (KV) store, written natively in Go.

It is meant to be used as a backend for the `raft` [package
here](https://github.com/hashicorp/raft).
