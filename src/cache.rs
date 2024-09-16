use std::hash::Hasher;

use crate::{ObjectHash, Stashable};

fn combine_hashes(hashes: &[ObjectHash]) -> ObjectHash {
    let mut hasher = seahash::SeaHasher::new();
    for hash in hashes {
        hasher.write_u64(hash.0);
    }
    ObjectHash(hasher.finish())
}

/// HashCache is the cached result of a function call that is only
/// evaluated lazily whenever the inputs have changed, according to their
/// ObjectHash.
pub struct HashCache<T> {
    /// The hash of the arguments for the cached value, if present
    hash: Option<ObjectHash>,

    /// The cached value
    value: Option<T>,
}

impl<T> HashCache<T> {
    /// Create a new RevisedProperty with an empty cache
    pub fn new() -> HashCache<T> {
        HashCache {
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
        A0: Stashable,
    {
        let current_hash = ObjectHash::from_stashable(&arg0);
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
        A0: Stashable,
        A1: Stashable,
    {
        let current_revision = combine_hashes(&[
            ObjectHash::from_stashable(&arg0),
            ObjectHash::from_stashable(&arg1),
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
        A0: Stashable,
        A1: Stashable,
        A2: Stashable,
    {
        let current_revision = combine_hashes(&[
            ObjectHash::from_stashable(&arg0),
            ObjectHash::from_stashable(&arg1),
            ObjectHash::from_stashable(&arg2),
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
        A0: Stashable,
        A1: Stashable,
        A2: Stashable,
        A3: Stashable,
    {
        let current_revision = combine_hashes(&[
            ObjectHash::from_stashable(&arg0),
            ObjectHash::from_stashable(&arg1),
            ObjectHash::from_stashable(&arg2),
            ObjectHash::from_stashable(&arg3),
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
        A0: Stashable,
        A1: Stashable,
        A2: Stashable,
        A3: Stashable,
        A4: Stashable,
    {
        let current_revision = combine_hashes(&[
            ObjectHash::from_stashable(&arg0),
            ObjectHash::from_stashable(&arg1),
            ObjectHash::from_stashable(&arg2),
            ObjectHash::from_stashable(&arg3),
            ObjectHash::from_stashable(&arg4),
        ]);
        if self.hash != Some(current_revision) {
            self.value = Some(f(arg0, arg1, arg2, arg3, arg4));
            self.hash = Some(current_revision);
        }
    }
}
