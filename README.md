# hashstash

It's like git but for your Rust data structures.

`hashstash` provides means to concisely hash and serialize data structures
and later de-serialize the same or new data structures, allowing you to
capture snapshots in time and rollback to or recreate those states later.

Types that you want to hash or serialize implement the `Stashable` trait,
which uses its single `stash` method to transparently do both hashing and
serializing. Stashed objects are stored in a `Stash` and referred to via a
`StashHandle<T>`. From a `StashHandle<T>`, you can recreate new instances
of `T` in the state it was stashed with that handle if `T` is `Unstashable`.
You can also modify the original object or other objects of type `T` in-place
to recreate the state the handle represents if `T` is `UnstashableInplace`.
Deserializing objects in place is risky, so `UnstashableInplace` uses a two-
phase approach where stashed contents are read through in a practice run
before being read through a second time when the object is actually modified.

Under the hood, `hashstash` implements a content-addressable storage system
in the form of the `Stash` struct, such that objects with identical hashes
are only serialized once. This automatically deduplicates serialized data
and means that the marginal cost of stashing the same objects multiple times
is free.
