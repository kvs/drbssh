# `drbssh`

A protocol implementation for Distributed Ruby (DRb), supporting SSH-connections.

## Usage

```ruby
	DRb.start_service 'drbssh://'
	remote = DRbObject.new_with_uri("drbssh://remote/")
	remote.eval('1+1')

	remote = DRbObject.new_with_uri("drbssh://remote/path/to/ruby")
```

## Description

`drbssh` makes it possible to create DRb-connections via SSH, with support for
bi-directional communication. In contrast to other DRb protocols, DRbSSH *requires*
that a local server is started before creating `DRbObject`s.

A newly-created `DRbObject` pointing at a remote server will be pointing at an
instance of `Binding`, representing the top-level of a newly-started Ruby
interpreter. The only interesting function exposed is `eval`.

## Development

Uses a Vagrant VM with an Ubuntu-installation to serve as the remote end-point in tests.

The Vagrant VM is supposed to be reachable as 'vagrant-drbssh'.

## TODOS

* Use Net::SSH if installed/possible?
