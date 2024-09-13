use std::{
    any::TypeId,
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

pub use stasher::Stasher;
use unstasher::{InplaceUnstashPhase, InplaceUnstasher, UnstasherBackend};
pub use unstasher::{UnstashError, Unstasher};
pub use valuetypes::{PrimitiveType, ValueType};

pub trait Stashable {
    fn stash(&self, stasher: &mut Stasher);
}

pub trait Unstashable: Sized {
    fn unstash(unstasher: &mut Unstasher) -> Result<Self, UnstashError>;
}

pub trait UnstashableInplace {
    fn unstash_inplace(&mut self, unstasher: &mut InplaceUnstasher) -> Result<(), UnstashError>;
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
struct TypeSaltedHash(u64);

impl TypeSaltedHash {
    fn hash_object<T: 'static + Stashable>(object: &T) -> TypeSaltedHash {
        let mut hasher = seahash::SeaHasher::new();

        TypeId::of::<T>().hash(&mut hasher);

        let mut stasher = Stasher::new_hasher(&mut hasher);

        object.stash(&mut stasher);

        TypeSaltedHash(hasher.finish())
    }
}

struct StashedObject {
    bytes: Vec<u8>,
    reference_count: Cell<u16>,
    dependencies: Vec<TypeSaltedHash>,
}

struct StashMap {
    objects: HashMap<TypeSaltedHash, StashedObject>,
}

impl StashMap {
    fn new() -> StashMap {
        StashMap {
            objects: HashMap::new(),
        }
    }

    fn stash_and_add_reference<'a, T: 'static + Stashable>(
        &'a mut self,
        object: &T,
    ) -> TypeSaltedHash {
        let hash = TypeSaltedHash::hash_object(object);

        if let Some(stashed_object) = self.objects.get(&hash) {
            stashed_object
                .reference_count
                .set(stashed_object.reference_count.get() + 1);
            return hash;
        }

        let mut dependencies = Vec::<TypeSaltedHash>::new();
        let mut bytes = Vec::<u8>::new();

        let mut stasher = Stasher::new_serializer(&mut bytes, &mut dependencies, self);

        object.stash(&mut stasher);

        let stashed_object = StashedObject {
            bytes,
            reference_count: Cell::new(1),
            dependencies,
        };
        self.objects.insert(hash, stashed_object);
        hash
    }

    fn add_reference(&self, hash: TypeSaltedHash) {
        let stashed_object = self.objects.get(&hash).unwrap();
        stashed_object
            .reference_count
            .set(stashed_object.reference_count.get() + 1);
    }

    fn unstash<'a, T: Unstashable>(&self, hash: TypeSaltedHash) -> Result<T, UnstashError> {
        let Some(stashed_object) = self.objects.get(&hash) else {
            // Is this ever possible?
            return Err(UnstashError::NotFound);
        };

        let mut stash_out =
            Unstasher::new(UnstasherBackend::from_stashed_object(stashed_object, self));

        let object = T::unstash(&mut stash_out)?;

        if !stash_out.backend().is_finished() {
            return Err(UnstashError::NotFinished);
        }

        Ok(object)
    }

    fn unstash_inplace<'a, T: UnstashableInplace>(
        &self,
        hash: TypeSaltedHash,
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

    fn remove_reference(&mut self, hash: TypeSaltedHash) {
        fn decrease_refcounts_recursive(
            stashmap: &StashMap,
            hash: TypeSaltedHash,
            objects_to_remove: &mut Vec<TypeSaltedHash>,
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

        let mut objects_to_remove: Vec<TypeSaltedHash> = Vec::new();

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

    pub fn stash<T: 'static + Stashable>(&self, object: &T) -> StashHandle<T> {
        let mut stashmap = self.map.borrow_mut();
        let hash = stashmap.stash_and_add_reference(object);
        StashHandle::new(Rc::clone(&self.map), hash)
    }

    pub fn unstash<T: Unstashable>(&self, handle: &StashHandle<T>) -> Result<T, UnstashError> {
        self.map.borrow().unstash(handle.hash)
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
    NotTheSame,
    ModifiedDuringValidation,
    SameHashAfterModifying,
}

pub fn test_stash_roundtrip<T: 'static + Stashable + Unstashable, Create, Modify>(
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

    let hash_after_modifying = TypeSaltedHash::hash_object(&object);

    if hash_after_modifying == handle_to_original.object_hash() {
        return Err(RoundTripError::SameHashAfterModifying);
    }

    let unstashed_object = stash
        .unstash(&handle_to_original)
        .map_err(|e| RoundTripError::BasicUnstashError(e))?;

    let hash_after_unstashing = TypeSaltedHash::hash_object(&unstashed_object);
    if hash_after_unstashing != handle_to_original.object_hash() {
        return Err(RoundTripError::NotTheSame);
    }

    Ok(())
}

pub fn test_stash_roundtrip_inplace<T: 'static + Stashable + UnstashableInplace, Create, Modify>(
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

    let hash_after_modifying = TypeSaltedHash::hash_object(&object);
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

    let hash_after_validation = TypeSaltedHash::hash_object(&object);
    if hash_after_validation != hash_before_validation {
        return Err(RoundTripError::ModifiedDuringValidation);
    }

    map.unstash_inplace(
        handle_to_original.hash,
        &mut object,
        InplaceUnstashPhase::Write,
    )
    .map_err(|e| RoundTripError::UncaughtUnstashError(e))?;

    let hash_after_write = TypeSaltedHash::hash_object(&object);
    if hash_after_write != handle_to_original.object_hash() {
        return Err(RoundTripError::NotTheSame);
    }

    Ok(())
}

pub struct StashHandle<T> {
    map: Rc<RefCell<StashMap>>,
    hash: TypeSaltedHash,
    _phantom_data: PhantomData<T>,
}

impl<T> StashHandle<T> {
    fn new(map: Rc<RefCell<StashMap>>, hash: TypeSaltedHash) -> StashHandle<T> {
        StashHandle {
            map,
            hash,
            _phantom_data: PhantomData,
        }
    }

    pub(crate) fn object_hash(&self) -> TypeSaltedHash {
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
