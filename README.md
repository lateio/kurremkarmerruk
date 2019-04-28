Kurremkarmerruk
=====

Kurremkarmerruk will be a standards compliant, reliable and extensible Domain Name System (DNS) server. It will be able to act as an authoritative and/or a caching and recursing name server, with different alternative backends for both zone and cache storage. At the time of writing, however, it lacks the testing – and in some cases implementation and clear design vision – to verify it as any of the aforementioned.

Kurremkarmerruk is written in [Erlang](http://erlang.org) because the language offers a well-established, proven model for concurrency and asynchrony, both properties which are useful for DNS.

Does it work? Is it usable?
---

My sample of N=1 concludes: "It (mostly) works on my home network, and also as an authoritative name server."

Honestly though, Kurremkarmerruk is a work in progress and has seen little use and even less testing. The overall design is still in flux in such a way that it is still unnecessarily hard to reason/verify that it works correctly. Additionally, it is currently inadequate for hostile environments (like the internet) as it does not yet implement any denial of service (DOS) protections.

At the time of writing (version 0.0.1) Kurremkarmerruk should thus be considered inadequately tested for important/serious deployments. Furthermore, the [documentation](doc/src/manual/index.asciidoc) does not currently support the setup, deployment and operation of Kurremkarmerruk by users without extensive prior knowledge of the project. Rectifying the lack of documentation is an important, ongoing priority.

Roadmap
---
For version 0.0.2
* Documentation
* Eunit test coverage
* Common test suites to verify standards compliance
* Dialyzer
* Cleaner (=understandable) kurremkarmerruk_recurse handling
* update opcode (?)

Further on down the road
* Work on an Erlang cluster (multiple nodes)
* DNSSEC
* Move from sockets having a single worker to a shared worker pool
* Recurse timeout as a behavior
* Cache policy as a behavior
* Cache and zone storage as a behavior

---

May you find something you could put in DNS for the max TTL.
