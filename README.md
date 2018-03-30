Kurremkarmerruk
=====

What
---

Kurremkarmerruk is a Domain Name System (DNS) server written in Erlang.

At the time of writing (version 0.0.0) Kurremkarmerruk should be considered inadequately tested for important/serious deployments. Furthermore, documentation is currently non-existent. Rectifying the lack of documentation is one of the main goals of 0.0.1.


Roadmap
---
For version 0.0.1
* Documentation
* Eunit test coverage
* Dialyzer
* Possibly zone transfer functionality.

Further on down the road
* Work on an Erlang cluster (multiple nodes)
* DNSSEC
* Move from sockets having a single worker to a shared worker pool.
* Recurse timeout as a behavior
* Cache policy as a behavior
* Cache and zone storage as a behavior


---

May you find something you could put in DNS for the max ttl.
