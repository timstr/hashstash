use std::{
    cell::Cell,
    hash::Hasher,
    ops::{Deref, DerefMut},
};

use crate::{
    InplaceUnstasher, ObjectHash, Stashable, Stasher, UnstashError, Unstashable,
    UnstashableInplace, Unstasher,
};

fn combine_hashes(hashes: &[ObjectHash]) -> ObjectHash {
    let mut hasher = seahash::SeaHasher::new();
    for hash in hashes {
        hasher.write_u64(hash.0);
    }
    ObjectHash(hasher.finish())
}

#[derive(Copy, Clone)]
struct HashCacheEntry {
    context_hash: ObjectHash,
    object_hash: ObjectHash,
}

/// HashCache is a wrapper around a Stashable object that caches
/// the hash value of that object between repeated non-mutable
/// accesses. Mutably accessing the stored object invalidates
/// the cached hash value, which is only recomputed as needed.
pub struct HashCache<T: ?Sized> {
    /// The cached hash
    // TODO: make this size adjustable?
    entries: [Cell<Option<HashCacheEntry>>; 2],

    /// The stored object
    value: T,
}

impl<T> HashCache<T> {
    /// Create a new HashCache with the given value.
    /// The hash is not yet computed or cached.
    pub fn new(value: T) -> HashCache<T> {
        HashCache {
            entries: [Cell::new(None), Cell::new(None)],
            value,
        }
    }
}

impl<T: ?Sized> Deref for HashCache<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T: ?Sized> DerefMut for HashCache<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // Invalidate the cached hash
        for entry in &self.entries {
            entry.set(None);
        }

        &mut self.value
    }
}

impl<C, T: ?Sized + Stashable<C>> Stashable<C> for HashCache<T>
where
    C: Stashable<()>,
{
    fn stash(&self, stasher: &mut Stasher<C>) {
        if stasher.hashing() {
            // If hashing, look for a cached hash or compute
            // and save it if not cached

            // hash the context
            let context_hash = ObjectHash::from_stashable(stasher.context());

            let mut next_empty_entry = None;

            // search for a matching entry
            for (i, entry) in self.entries.iter().enumerate() {
                if let Some(entry) = entry.get() {
                    if entry.context_hash == context_hash {
                        stasher.u64(entry.object_hash.0);
                        return;
                    }
                } else if next_empty_entry.is_none() {
                    next_empty_entry = Some(i);
                }
            }

            // otherwise, if no matching entry was found,
            // recompute the object hash and store it

            let object_hash =
                ObjectHash::from_stashable_and_context(&self.value, stasher.context());

            self.entries[next_empty_entry.unwrap_or(0)].set(Some(HashCacheEntry {
                context_hash,
                object_hash,
            }));

            stasher.u64(object_hash.0);
        } else {
            // Otherwise, if serializing, just serialize
            self.deref().stash(stasher);
        }
    }
}

impl<C, T: Unstashable<C>> Unstashable<C> for HashCache<T> {
    fn unstash(unstasher: &mut Unstasher<C>) -> Result<Self, UnstashError> {
        Ok(HashCache::new(T::unstash(unstasher)?))
    }
}

impl<C, T: UnstashableInplace<C>> UnstashableInplace<C> for HashCache<T> {
    fn unstash_inplace(&mut self, unstasher: &mut InplaceUnstasher<C>) -> Result<(), UnstashError> {
        self.deref_mut().unstash_inplace(unstasher)
    }
}

/// HashCacheProperty is the cached result of a function call that is only
/// evaluated lazily whenever the inputs have changed, according to their
/// ObjectHash.
pub struct HashCacheProperty<T> {
    /// The hash of the arguments for the cached value, if present
    hash: Option<ObjectHash>,

    /// The cached value
    value: Option<T>,
}

impl<T> HashCacheProperty<T> {
    /// Create a new HashCacheProperty with an empty cache
    pub fn new() -> HashCacheProperty<T> {
        HashCacheProperty {
            hash: None,
            value: None,
        }
    }

    /// Get the cached value, which might not be filled yet.
    /// This stores the result of the refresh* method that
    /// was most recently called, if any.
    pub fn get_cached(&self) -> Option<&T> {
        self.value.as_ref()
    }

    /// Update the cache to store the result of calling f(arg0).
    /// If the function's output from the same arguments is already
    /// cached, the function is not called and the cache is kept.
    /// Otherwise, f is called and the cache is written to.
    /// f is assumed to be a pure function.
    pub fn refresh1<F, A0>(&mut self, f: F, arg0: A0)
    where
        F: Fn(A0) -> T,
        A0: Stashable<()>,
    {
        self.refresh1_with_context(f, arg0, &());
    }

    pub fn refresh1_with_context<C, F, A0>(&mut self, f: F, arg0: A0, context: &C)
    where
        F: Fn(A0) -> T,
        A0: Stashable<C>,
    {
        let current_hash = ObjectHash::from_stashable_and_context(&arg0, context);
        if self.hash != Some(current_hash) {
            self.value = Some(f(arg0));
            self.hash = Some(current_hash);
        }
    }

    /// Update the cache to store the result of calling f(arg0, arg1).
    /// If the function's output from the same arguments is already
    /// cached, the function is not called and the cache is kept.
    /// Otherwise, f is called and the cache is written to.
    /// f is assumed to be a pure function.
    pub fn refresh2<F, A0, A1>(&mut self, f: F, arg0: A0, arg1: A1)
    where
        F: Fn(A0, A1) -> T,
        A0: Stashable<()>,
        A1: Stashable<()>,
    {
        self.refresh2_with_context(f, arg0, arg1, &());
    }

    pub fn refresh2_with_context<C, F, A0, A1>(&mut self, f: F, arg0: A0, arg1: A1, context: &C)
    where
        F: Fn(A0, A1) -> T,
        A0: Stashable<C>,
        A1: Stashable<C>,
    {
        let current_revision = combine_hashes(&[
            ObjectHash::from_stashable_and_context(&arg0, context),
            ObjectHash::from_stashable_and_context(&arg1, context),
        ]);
        if self.hash != Some(current_revision) {
            self.value = Some(f(arg0, arg1));
            self.hash = Some(current_revision);
        }
    }

    /// Update the cache to store the result of calling f(arg0, arg1, arg2).
    /// If the function's output from the same arguments is already
    /// cached, the function is not called and the cache is kept.
    /// Otherwise, f is called and the cache is written to.
    /// f is assumed to be a pure function.
    pub fn refresh3<F, A0, A1, A2>(&mut self, f: F, arg0: A0, arg1: A1, arg2: A2)
    where
        F: Fn(A0, A1, A2) -> T,
        A0: Stashable<()>,
        A1: Stashable<()>,
        A2: Stashable<()>,
    {
        self.refresh3_with_context(f, arg0, arg1, arg2, &());
    }

    pub fn refresh3_with_context<C, F, A0, A1, A2>(
        &mut self,
        f: F,
        arg0: A0,
        arg1: A1,
        arg2: A2,
        context: &C,
    ) where
        F: Fn(A0, A1, A2) -> T,
        A0: Stashable<C>,
        A1: Stashable<C>,
        A2: Stashable<C>,
    {
        let current_revision = combine_hashes(&[
            ObjectHash::from_stashable_and_context(&arg0, context),
            ObjectHash::from_stashable_and_context(&arg1, context),
            ObjectHash::from_stashable_and_context(&arg2, context),
        ]);
        if self.hash != Some(current_revision) {
            self.value = Some(f(arg0, arg1, arg2));
            self.hash = Some(current_revision);
        }
    }

    /// Update the cache to store the result of calling f(arg0, arg1, arg2, arg3).
    /// If the function's output from the same arguments is already
    /// cached, the function is not called and the cache is kept.
    /// Otherwise, f is called and the cache is written to.
    /// f is assumed to be a pure function.
    pub fn refresh4<F, A0, A1, A2, A3>(&mut self, f: F, arg0: A0, arg1: A1, arg2: A2, arg3: A3)
    where
        F: Fn(A0, A1, A2, A3) -> T,
        A0: Stashable<()>,
        A1: Stashable<()>,
        A2: Stashable<()>,
        A3: Stashable<()>,
    {
        self.refresh4_with_context(f, arg0, arg1, arg2, arg3, &());
    }

    pub fn refresh4_with_context<C, F, A0, A1, A2, A3>(
        &mut self,
        f: F,
        arg0: A0,
        arg1: A1,
        arg2: A2,
        arg3: A3,
        context: &C,
    ) where
        F: Fn(A0, A1, A2, A3) -> T,
        A0: Stashable<C>,
        A1: Stashable<C>,
        A2: Stashable<C>,
        A3: Stashable<C>,
    {
        let current_revision = combine_hashes(&[
            ObjectHash::from_stashable_and_context(&arg0, context),
            ObjectHash::from_stashable_and_context(&arg1, context),
            ObjectHash::from_stashable_and_context(&arg2, context),
            ObjectHash::from_stashable_and_context(&arg3, context),
        ]);
        if self.hash != Some(current_revision) {
            self.value = Some(f(arg0, arg1, arg2, arg3));
            self.hash = Some(current_revision);
        }
    }

    /// Update the cache to store the result of calling f(arg0, arg1, arg2, arg3, arg4).
    /// If the function's output from the same arguments is already
    /// cached, the function is not called and the cache is kept.
    /// Otherwise, f is called and the cache is written to.
    /// f is assumed to be a pure function.
    pub fn refresh5<F, A0, A1, A2, A3, A4>(
        &mut self,
        f: F,
        arg0: A0,
        arg1: A1,
        arg2: A2,
        arg3: A3,
        arg4: A4,
    ) where
        F: Fn(A0, A1, A2, A3, A4) -> T,
        A0: Stashable<()>,
        A1: Stashable<()>,
        A2: Stashable<()>,
        A3: Stashable<()>,
        A4: Stashable<()>,
    {
        self.refresh5_with_context(f, arg0, arg1, arg2, arg3, arg4, &());
    }

    pub fn refresh5_with_context<C, F, A0, A1, A2, A3, A4>(
        &mut self,
        f: F,
        arg0: A0,
        arg1: A1,
        arg2: A2,
        arg3: A3,
        arg4: A4,
        context: &C,
    ) where
        F: Fn(A0, A1, A2, A3, A4) -> T,
        A0: Stashable<C>,
        A1: Stashable<C>,
        A2: Stashable<C>,
        A3: Stashable<C>,
        A4: Stashable<C>,
    {
        let current_revision = combine_hashes(&[
            ObjectHash::from_stashable_and_context(&arg0, context),
            ObjectHash::from_stashable_and_context(&arg1, context),
            ObjectHash::from_stashable_and_context(&arg2, context),
            ObjectHash::from_stashable_and_context(&arg3, context),
            ObjectHash::from_stashable_and_context(&arg4, context),
        ]);
        if self.hash != Some(current_revision) {
            self.value = Some(f(arg0, arg1, arg2, arg3, arg4));
            self.hash = Some(current_revision);
        }
    }
}
