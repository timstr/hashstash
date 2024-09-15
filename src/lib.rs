use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    hash::{Hash, Hasher},
    marker::PhantomData,
    rc::Rc,
};

mod stasher;
mod unstasher;
mod valuetypes;

#[cfg(test)]
mod test;

pub use stasher::{Order, Stasher};
pub use unstasher::{InplaceUnstashPhase, InplaceUnstasher, UnstashError, Unstasher};
pub use valuetypes::{PrimitiveType, ValueType};

use unstasher::UnstasherBackend;

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
struct ObjectHash(u64);

impl ObjectHash {
    fn from_stashable<T: Stashable>(object: &T) -> ObjectHash {
        Self::with_stasher(|stasher| object.stash(stasher))
    }

    fn with_stasher<F: FnMut(&mut Stasher)>(mut f: F) -> ObjectHash {
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

    fn add_reference(&self, hash: ObjectHash) {
        let stashed_object = self.objects.get(&hash).unwrap();
        stashed_object
            .reference_count
            .set(stashed_object.reference_count.get() + 1);
    }

    fn unstash<'a, R, F: FnMut(&mut Unstasher) -> Result<R, UnstashError>>(
        &self,
        hash: ObjectHash,
        mut f: F,
    ) -> Result<R, UnstashError> {
        let Some(stashed_object) = self.objects.get(&hash) else {
            // Is this ever possible?
            return Err(UnstashError::NotFound);
        };

        let mut stash_out =
            Unstasher::new(UnstasherBackend::from_stashed_object(stashed_object, self));

        let result = f(&mut stash_out)?;

        if !stash_out.backend().is_finished() {
            return Err(UnstashError::NotFinished);
        }

        Ok(result)
    }

    fn unstash_inplace<'a, T: UnstashableInplace>(
        &self,
        hash: ObjectHash,
        object: &mut T,
        phase: InplaceUnstashPhase,
    ) -> Result<(), UnstashError> {
        let Some(stashed_object) = self.objects.get(&hash) else {
            // Is this ever possible?
            return Err(UnstashError::NotFound);
        };

        let mut stash_out = InplaceUnstasher::new(
            UnstasherBackend::from_stashed_object(stashed_object, self),
            phase,
        );

        object.unstash_inplace(&mut stash_out)?;

        if !stash_out.backend().is_finished() {
            return Err(UnstashError::NotFinished);
        }

        Ok(())
    }

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

pub struct Stash {
    map: Rc<RefCell<StashMap>>,
}

impl Stash {
    // TODO: add the ability to save *one snapshot* to disk

    pub fn new() -> Stash {
        Stash {
            map: Rc::new(RefCell::new(StashMap::new())),
        }
    }

    pub fn num_objects(&self) -> usize {
        self.map.borrow().objects.len()
    }

    pub fn stash<T: Stashable>(&self, object: &T) -> StashHandle<T> {
        let mut stashmap = self.map.borrow_mut();
        let hash = stashmap.stash_and_add_reference(|stasher| object.stash(stasher));
        StashHandle::new(Rc::clone(&self.map), hash)
    }

    pub fn unstash<T: Unstashable>(&self, handle: &StashHandle<T>) -> Result<T, UnstashError> {
        self.map.borrow().unstash(handle.hash, T::unstash)
    }

    pub fn unstash_inplace<T: UnstashableInplace>(
        &self,
        handle: &StashHandle<T>,
        object: &mut T,
    ) -> Result<(), UnstashError> {
        let map = self.map.borrow();
        map.unstash_inplace(handle.hash, object, InplaceUnstashPhase::Validate)?;
        map.unstash_inplace(handle.hash, object, InplaceUnstashPhase::Write)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum RoundTripError {
    BasicUnstashError(UnstashError),
    UncaughtUnstashError(UnstashError),
    DifferentHashAfterUnstashing,
    ModifiedDuringValidation,
    SameHashAfterModifying,
}

pub fn test_stash_roundtrip<T: Stashable + Unstashable, Create, Modify>(
    mut create: Create,
    mut modify_object: Modify,
) -> Result<(), RoundTripError>
where
    Create: FnMut() -> T,
    Modify: FnMut(&mut T),
{
    let mut object = create();

    let stash = Stash::new();
    let handle_to_original = stash.stash(&object);

    modify_object(&mut object);

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
        &mut object,
        InplaceUnstashPhase::Validate,
    )
    .map_err(|e| RoundTripError::BasicUnstashError(e))?;

    let hash_after_validation = ObjectHash::from_stashable(&object);
    if hash_after_validation != hash_before_validation {
        return Err(RoundTripError::ModifiedDuringValidation);
    }

    map.unstash_inplace(
        handle_to_original.hash,
        &mut object,
        InplaceUnstashPhase::Write,
    )
    .map_err(|e| RoundTripError::UncaughtUnstashError(e))?;

    let hash_after_write = ObjectHash::from_stashable(&object);
    if hash_after_write != handle_to_original.object_hash() {
        return Err(RoundTripError::DifferentHashAfterUnstashing);
    }

    Ok(())
}

pub struct StashHandle<T> {
    map: Rc<RefCell<StashMap>>,
    hash: ObjectHash,
    _phantom_data: PhantomData<T>,
}

impl<T> StashHandle<T> {
    fn new(map: Rc<RefCell<StashMap>>, hash: ObjectHash) -> StashHandle<T> {
        StashHandle {
            map,
            hash,
            _phantom_data: PhantomData,
        }
    }

    pub(crate) fn object_hash(&self) -> ObjectHash {
        self.hash
    }

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

impl<T> Drop for StashHandle<T> {
    fn drop(&mut self) {
        let mut map = self.map.borrow_mut();
        map.remove_reference(self.hash);
    }
}
