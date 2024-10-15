use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    hash::{Hash, Hasher},
    marker::PhantomData,
    rc::Rc,
};

mod cache;
mod stasher;
mod unstasher;
mod valuetypes;

#[cfg(test)]
mod test;

pub use cache::HashCache;
pub use stasher::{Order, Stasher};
pub use unstasher::{InplaceUnstasher, UnstashError, Unstasher};
pub use valuetypes::{PrimitiveType, ValueType};

use unstasher::{InplaceUnstashPhase, UnstasherBackend};

/// Trait for hashing and serializing an object
pub trait Stashable {
    /// Stash the object. The given Stasher may hash or serialize
    /// the data it's given, but this is transparent to the user.
    ///
    /// During typical use, this method is called twice, once to
    /// hash the object's contents and find a matching stashed
    /// object, and a second time to serialize the same contents
    /// to create a new stashed object if no match yet exists.
    fn stash(&self, stasher: &mut Stasher);
}

impl<T: Stashable> Stashable for &T {
    fn stash(&self, stasher: &mut Stasher) {
        T::stash(self, stasher);
    }
}

/// Trait for objects that can be unstashed or deserialized by
/// creating a new object.
pub trait Unstashable: Sized {
    /// Unstash/deserialize a new object.
    /// This method is called only once per object being unstashed.
    ///
    /// Consider using [test_stash_roundtrip] to test whether
    /// this method and the corresponding [Stashable] implementation
    /// are behaving correctly.
    fn unstash(unstasher: &mut Unstasher) -> Result<Self, UnstashError>;
}

/// Trait for objects that can be unstashed or deserialized by
/// modifying an existing object.
pub trait UnstashableInplace {
    /// Unstash/deserialize an existing object, either validating
    /// the data being unstashed without making changes to the
    /// object, OR reading the same data and writing it to the object.
    ///
    /// This method is called in two phases. The first is the validation
    /// phase, in which contents are unstashed without being written
    /// to the object, as a sort of practice run to detect errors.
    /// The second phase is the write phase, in which the same contents
    /// are unstashed a second time but actually written to the object.
    ///
    /// This two-phase approach allows unstashing errors to be caught
    /// without leaving an object in a partially-modified state. While
    /// it may seem subtle and confusing, nearly all methods of
    /// [InplaceUnstasher] handle this transparently.
    ///
    /// Consider using [test_stash_roundtrip_inplace] to test whether
    /// this method and the corresponding [Stashable] implementation
    /// are behaving correctly.
    fn unstash_inplace(&mut self, unstasher: &mut InplaceUnstasher) -> Result<(), UnstashError>;
}

/// A small and fixed-size summary of the contents to an object,
/// such that changes to an object result in a different ObjectHash.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct ObjectHash(u64);

impl ObjectHash {
    /// Create a new ObjectHash by hashing a Stashable object
    pub fn from_stashable<T: Stashable>(object: &T) -> ObjectHash {
        Self::with_stasher(|stasher| object.stash(stasher))
    }

    /// Create a new ObjectHash by hashing the data given to
    /// a Stasher in the provided function
    pub fn with_stasher<F: FnMut(&mut Stasher)>(mut f: F) -> ObjectHash {
        let mut hasher = seahash::SeaHasher::new();

        let mut stasher = Stasher::new_hasher(&mut hasher);

        f(&mut stasher);

        ObjectHash(hasher.finish())
    }
}

/// The serialized contents of an object and the hashes of the objects
/// it depends on, intended to be stored in a [StashMap]
struct StashedObject {
    bytes: Vec<u8>,
    reference_count: Cell<u16>,
    dependencies: Vec<ObjectHash>,
}

/// A container storing stashed objects by the hashes of their contents
struct StashMap {
    objects: HashMap<ObjectHash, StashedObject>,
}

impl StashMap {
    /// Create a new empty StashMap
    fn new() -> StashMap {
        StashMap {
            objects: HashMap::new(),
        }
    }

    /// Stash an object. The object is first hashed. If the hash doesn't
    /// match any existing objects, the object is serialized and its serialized
    /// contents are stored in the stashmap with an initial reference count of
    /// one. Otherwise, if the hash matches an existing serialized object, it
    /// is not serialized a second time and the existing object has its reference
    /// count increased.
    fn stash_and_add_reference<F: FnMut(&mut Stasher)>(&mut self, mut f: F) -> ObjectHash {
        let hash = ObjectHash::with_stasher(&mut f);

        if let Some(stashed_object) = self.objects.get(&hash) {
            stashed_object
                .reference_count
                .set(stashed_object.reference_count.get() + 1);
            return hash;
        }

        let mut dependencies = Vec::<ObjectHash>::new();
        let mut bytes = Vec::<u8>::new();

        let mut stasher = Stasher::new_serializer(&mut bytes, &mut dependencies, self);

        f(&mut stasher);

        let stashed_object = StashedObject {
            bytes,
            reference_count: Cell::new(1),
            dependencies,
        };
        self.objects.insert(hash, stashed_object);
        hash
    }

    /// Increase the reference count of an existing stashed object.
    /// This method panics if no object with the given hash exists.
    fn add_reference(&self, hash: ObjectHash) {
        let stashed_object = self.objects.get(&hash).unwrap();
        stashed_object
            .reference_count
            .set(stashed_object.reference_count.get() + 1);
    }

    /// Unstash/deserialize an object by finding an existing stashed
    /// object for the given hash and passing an [Unstasher] with
    /// its contents to the given function.
    /// This method panics if there is not stashed object with the
    /// given hash.
    fn unstash<'a, R, F: FnMut(&mut Unstasher) -> Result<R, UnstashError>>(
        &self,
        hash: ObjectHash,
        mut f: F,
    ) -> Result<R, UnstashError> {
        let stashed_object = self.objects.get(&hash).unwrap();

        let mut unstasher =
            Unstasher::new(UnstasherBackend::from_stashed_object(stashed_object, self));

        let result = f(&mut unstasher)?;

        if !unstasher.backend().is_finished() {
            return Err(UnstashError::NotFinished);
        }

        Ok(result)
    }

    /// Unstash/deserialize an object by finding an existing stashed
    /// object for the given hash and then calling the object's
    /// [UnstashableInplace::unstash_inplace] method with the given
    /// phase.
    /// This method panics if there is not stashed object with the
    /// given hash.
    fn unstash_inplace<'a, F: FnMut(&mut InplaceUnstasher) -> Result<(), UnstashError>>(
        &self,
        hash: ObjectHash,
        phase: InplaceUnstashPhase,
        mut f: F,
    ) -> Result<(), UnstashError> {
        let stashed_object = self.objects.get(&hash).unwrap();

        let mut unstasher = InplaceUnstasher::new(
            UnstasherBackend::from_stashed_object(stashed_object, self),
            phase,
        );

        f(&mut unstasher)?;

        if !unstasher.backend().is_finished() {
            return Err(UnstashError::NotFinished);
        }

        Ok(())
    }

    /// Decrease the reference count of the stashed object,
    /// removing it from the StashMap if its reference count
    /// reaches zero and recursively removing references from
    /// its dependencies as needed.
    /// This method panics if no stashed object with the given
    /// hash exists.
    fn remove_reference(&mut self, hash: ObjectHash) {
        fn decrease_refcounts_recursive(
            stashmap: &StashMap,
            hash: ObjectHash,
            objects_to_remove: &mut Vec<ObjectHash>,
        ) {
            let object = stashmap.objects.get(&hash).unwrap();
            let mut refcount = object.reference_count.get();
            debug_assert!(refcount > 0);
            refcount -= 1;
            object.reference_count.set(refcount);
            if refcount == 0 {
                objects_to_remove.push(hash);
                for dependency in &object.dependencies {
                    decrease_refcounts_recursive(stashmap, *dependency, objects_to_remove);
                }
            }
        }

        let mut objects_to_remove: Vec<ObjectHash> = Vec::new();

        decrease_refcounts_recursive(self, hash, &mut objects_to_remove);

        for hash in objects_to_remove {
            self.objects.remove(&hash).unwrap();
        }
    }
}

/// A container storing the serialized contents of stashed objects
/// in a deduplicated manner, with which new objects can recreated
/// from past snapshots and with which existing objects can be rolled
/// back to a different state.
///
/// Objects that are stashed should implement [Stashable] and at
/// least of [Unstashable] and [UnstashableInplace].
pub struct Stash {
    map: Rc<RefCell<StashMap>>,
}

impl Stash {
    // TODO: add the ability to save *one snapshot* to disk

    /// Create a new empty Stash
    pub fn new() -> Stash {
        Stash {
            map: Rc::new(RefCell::new(StashMap::new())),
        }
    }

    /// Get the number of objects stored in the stash.
    /// Due to deduplication, this may be less than the
    /// number of objects that have been stashed overall.
    pub fn num_objects(&self) -> usize {
        self.map.borrow().objects.len()
    }

    /// Stash an object, and get a [StashHandle] to its stashed contents
    /// so that it can be unstashed again later.
    ///
    /// The object is hashed and serialized and stored in the Stash.
    /// If an existing object has the same contents, its storage
    /// is reused and the serialization is skipped.
    pub fn stash<T: Stashable>(&self, object: &T) -> StashHandle<T> {
        let mut stashmap = self.map.borrow_mut();
        let hash = stashmap.stash_and_add_reference(|stasher| object.stash(stasher));
        StashHandle::new(Rc::clone(&self.map), hash)
    }

    /// Unstash a new object to deserialize and recreate the state of an
    /// object that was previously stashed, as represented by the given
    /// [StashHandle].
    ///
    /// See [Unstashable], which is needed to use this method, or else
    /// see [Self::unstash_inplace] and [UnstashableInplace] to unstash
    /// and restore existing objects to a different state.
    pub fn unstash<T: Unstashable>(&self, handle: &StashHandle<T>) -> Result<T, UnstashError> {
        self.map.borrow().unstash(handle.hash, T::unstash)
    }

    /// Unstash a new object to deserialize and recreate a previously-
    /// stashed object with the given [StashHandle], but using a custom
    /// function to do the unstashing. Use this if unstashing depends
    /// on additional data that can't be passed through the existing
    /// [Unstashable] interface.
    pub fn unstash_proxy<T, F>(&self, handle: &StashHandle<T>, f: F) -> Result<T, UnstashError>
    where
        F: FnMut(&mut Unstasher) -> Result<T, UnstashError>,
    {
        self.map.borrow().unstash(handle.hash, f)
    }

    /// Unstash an existing object to deserialize and restore the state
    /// of a previously stashed object, as represented by the given
    /// [StashHandle]. This method uses a two-phase approach to validate
    /// the data being deserialized before the object is modified, to
    /// avoid leaving an object in a partially-written state.
    ///
    /// See [UnstashableInplace], which is needed to use this method, or
    /// else see [Self::unstash] and [Unstashable] to unstash newly-created
    /// objects instead.
    pub fn unstash_inplace<T: UnstashableInplace>(
        &self,
        handle: &StashHandle<T>,
        object: &mut T,
    ) -> Result<(), UnstashError> {
        let map = self.map.borrow();
        map.unstash_inplace(handle.hash, InplaceUnstashPhase::Validate, |unstasher| {
            object.unstash_inplace(unstasher)
        })?;
        map.unstash_inplace(handle.hash, InplaceUnstashPhase::Write, |unstasher| {
            object.unstash_inplace(unstasher)
        })
    }
}

/// Errors that can happen during one of the round trip tests,
/// which indicate a bug in an object's stashing and unstashing
/// implementations. See each variant's documentation for
/// explanations about how these bugs can be fixed.
///
/// See [test_stash_roundtrip] and [test_stash_roundtrip_inplace].
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum RoundTripError {
    /// The object could not be unstashed because the contents
    /// that were stashed do not match the type of contents being
    /// unstashed. The contents must be stashed and unstashed
    /// in the same order and with the same types.
    BasicUnstashError(UnstashError),

    /// The object encountered an error during the write phase
    /// of in-place unstashing which was not caught during the
    /// validation phase. The object must unstash the same
    /// contents during the validation and write phases of its
    /// [UnstashableInplace::unstash_inplace] method.
    UncaughtUnstashError(UnstashError),

    /// The object was stashed and unstashed without obvious
    /// error, but the object hashes do not agree before and
    /// after unstashing. All important object contents must
    /// be stashed with the correct type and order and restored
    /// during unstashing.
    DifferentHashAfterUnstashing,

    /// The object was modified during the validation phase of
    /// its [UnstashableInplace::unstash_inplace] method. This
    /// phase needs to unstash the same contents as the write
    /// phase but should not modify the object.
    ModifiedDuringValidation,

    /// The object was allegedly modified, but it produced the
    /// same hash before and after the modification. The given
    /// modification function needs to modify the object in a
    /// manner that will make it logically distinct from other
    /// objects of its type, and the object needs to stash all
    /// contents that are similarly important to its identity.
    SameHashAfterModifying,
}

/// Perform an end-to-end test of a [Stashable] object which
/// implements [Unstashable]. The given `create` function
/// must produce a new instance of the desired object type and
/// the given `modify` function must mutate that same object
/// such that it is logically distinct afterwards and hashes
/// to a different value. See [RoundTripError] for possible
/// failures and their explanations.
///
/// It is recommended to call this method in unit tests with
/// multiple different initial values and modifications.
/// Successful round-trip tests will return `Ok(())`.
pub fn test_stash_roundtrip<T: Stashable + Unstashable, Create, Modify>(
    mut create: Create,
    mut modify: Modify,
) -> Result<(), RoundTripError>
where
    Create: FnMut() -> T,
    Modify: FnMut(&mut T),
{
    let mut object = create();

    let stash = Stash::new();
    let handle_to_original = stash.stash(&object);

    modify(&mut object);

    let hash_after_modifying = ObjectHash::from_stashable(&object);

    if hash_after_modifying == handle_to_original.object_hash() {
        return Err(RoundTripError::SameHashAfterModifying);
    }

    let unstashed_object = stash
        .unstash(&handle_to_original)
        .map_err(|e| RoundTripError::BasicUnstashError(e))?;

    let hash_after_unstashing = ObjectHash::from_stashable(&unstashed_object);
    if hash_after_unstashing != handle_to_original.object_hash() {
        return Err(RoundTripError::DifferentHashAfterUnstashing);
    }

    Ok(())
}

/// Perform an end-to-end test of a [Stashable] object which
/// implements [UnstashableInplace]. The given `create` function
/// must produce a new instance of the desired object type and
/// the given `modify` function must mutate that same object
/// such that it is logically distinct afterwards and hashes
/// to a different value. See [RoundTripError] for possible
/// failures and their explanations.
///
/// It is recommended to call this method in unit tests with
/// multiple different initial values and modifications.
/// Successful round-trip tests will return `Ok(())`.
pub fn test_stash_roundtrip_inplace<T: Stashable + UnstashableInplace, Create, Modify>(
    mut create: Create,
    mut modify: Modify,
) -> Result<(), RoundTripError>
where
    Create: FnMut() -> T,
    Modify: FnMut(&mut T),
{
    let mut object = create();

    let stash = Stash::new();
    let handle_to_original = stash.stash(&object);

    modify(&mut object);

    let hash_after_modifying = ObjectHash::from_stashable(&object);
    if hash_after_modifying == handle_to_original.object_hash() {
        return Err(RoundTripError::SameHashAfterModifying);
    }

    let hash_before_validation = hash_after_modifying;

    let map = stash.map.borrow();
    map.unstash_inplace(
        handle_to_original.hash,
        InplaceUnstashPhase::Validate,
        |unstasher| object.unstash_inplace(unstasher),
    )
    .map_err(|e| RoundTripError::BasicUnstashError(e))?;

    let hash_after_validation = ObjectHash::from_stashable(&object);
    if hash_after_validation != hash_before_validation {
        return Err(RoundTripError::ModifiedDuringValidation);
    }

    map.unstash_inplace(
        handle_to_original.hash,
        InplaceUnstashPhase::Write,
        |unstasher| object.unstash_inplace(unstasher),
    )
    .map_err(|e| RoundTripError::UncaughtUnstashError(e))?;

    let hash_after_write = ObjectHash::from_stashable(&object);
    if hash_after_write != handle_to_original.object_hash() {
        return Err(RoundTripError::DifferentHashAfterUnstashing);
    }

    Ok(())
}

/// A handle to a shared stashed object living in a [Stash].
/// Holding this handle ensures that the stash object is
/// not cleaned up, and dropping this handle may result in
/// the stashed object being removed from the stash.
pub struct StashHandle<T> {
    map: Rc<RefCell<StashMap>>,
    hash: ObjectHash,
    _phantom_data: PhantomData<T>,
}

impl<T> StashHandle<T> {
    /// Create a new handle
    fn new(map: Rc<RefCell<StashMap>>, hash: ObjectHash) -> StashHandle<T> {
        StashHandle {
            map,
            hash,
            _phantom_data: PhantomData,
        }
    }

    /// Get the hash of the stashed object
    pub fn object_hash(&self) -> ObjectHash {
        self.hash
    }

    /// Get the reference count of the stashed object
    #[cfg(test)]
    pub(crate) fn reference_count(&self) -> u16 {
        self.map
            .borrow()
            .objects
            .get(&self.hash)
            .unwrap()
            .reference_count
            .get()
    }
}

/// Cloning a StashHandle increases its reference count
impl<T> Clone for StashHandle<T> {
    fn clone(&self) -> Self {
        self.map.borrow().add_reference(self.hash);
        Self {
            map: Rc::clone(&self.map),
            hash: self.hash,
            _phantom_data: PhantomData,
        }
    }
}

/// Dropping a StashHandle decreases its reference count
impl<T> Drop for StashHandle<T> {
    fn drop(&mut self) {
        let mut map = self.map.borrow_mut();
        map.remove_reference(self.hash);
    }
}

/// Stash an object and immediately unstash it out-of-place, effectively
/// performing a deep clone of the object. This can be used where
/// implementing Clone is difficult (e.g. involving trait objects and
/// factories) and/or would duplicate logic already needed for stashing
/// and unstashing.
///
/// On success, returns both the cloned object and a handle to its stashed
/// data so that serialized contents can be reused if desired. The additional
/// memory usage of this method is lowered if the provided stash already
/// contains copies of sub-bojects being stashed.
pub fn stash_clone<T>(object: &T, stash: &Stash) -> Result<(T, StashHandle<T>), UnstashError>
where
    T: Stashable + Unstashable,
{
    let handle = stash.stash(object);

    match stash.unstash(&handle) {
        Ok(new_obj) => Ok((new_obj, handle)),
        Err(err) => Err(err),
    }
}

pub fn stash_clone_proxy<T, F>(
    object: &T,
    stash: &Stash,
    f: F,
) -> Result<(T, StashHandle<T>), UnstashError>
where
    T: Stashable,
    F: FnMut(&mut Unstasher) -> Result<T, UnstashError>,
{
    let handle = stash.stash(object);

    match stash.unstash_proxy(&handle, f) {
        Ok(new_obj) => Ok((new_obj, handle)),
        Err(err) => Err(err),
    }
}
