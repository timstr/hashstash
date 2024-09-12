use std::hash::Hasher;

use crate::{valuetypes::PrimitiveReadWrite, StashMap, Stashable, TypeSaltedHash, ValueType};

struct HashingStasher<'a> {
    hasher: &'a mut seahash::SeaHasher,
}

struct SerializingStasher<'a> {
    data: &'a mut Vec<u8>,
    dependencies: &'a mut Vec<TypeSaltedHash>,
    stashmap: &'a mut StashMap,
}

enum StasherBackend<'a> {
    Hash(HashingStasher<'a>),
    Serialize(SerializingStasher<'a>),
}

impl<'a> StasherBackend<'a> {
    fn write_raw_bytes(&mut self, bytes: &[u8]) {
        match self {
            StasherBackend::Hash(hash) => hash.hasher.write(bytes),
            StasherBackend::Serialize(serialize) => serialize.data.extend_from_slice(bytes),
        }
    }

    fn stash_dependency<T: 'static + Stashable>(&mut self, object: &T) -> TypeSaltedHash {
        match self {
            StasherBackend::Hash(_) => TypeSaltedHash::hash_object(object),
            StasherBackend::Serialize(serializer) => {
                let hash = serializer.stashmap.stash_and_add_reference(object);
                serializer.dependencies.push(hash);
                hash
            }
        }
    }
}

pub struct Stasher<'a> {
    backend: StasherBackend<'a>,
}

/// Private methods
impl<'a> Stasher<'a> {
    pub(crate) fn new_serializer(
        data: &'a mut Vec<u8>,
        dependencies: &'a mut Vec<TypeSaltedHash>,
        stashmap: &'a mut StashMap,
    ) -> Stasher<'a> {
        Stasher {
            backend: StasherBackend::Serialize(SerializingStasher {
                data,
                dependencies,
                stashmap,
            }),
        }
    }

    pub(crate) fn new_hasher(hasher: &'a mut seahash::SeaHasher) -> Stasher<'a> {
        Stasher {
            backend: StasherBackend::Hash(HashingStasher { hasher }),
        }
    }

    pub(crate) fn write_raw_bytes(&mut self, bytes: &[u8]) {
        self.backend.write_raw_bytes(bytes);
    }

    /// Helper method to write a primitive
    pub(crate) fn write_primitive<T: PrimitiveReadWrite>(&mut self, x: T) {
        self.write_raw_bytes(&[ValueType::Primitive(T::TYPE).to_byte()]);
        x.write_raw_bytes_to(self);
    }

    /// Helper method to write a slice of primitives
    pub(crate) fn write_primitive_array_slice<T: PrimitiveReadWrite>(&mut self, x: &[T]) {
        self.write_raw_bytes(&[ValueType::Array(T::TYPE).to_byte()]);
        let len = x.len() as u32;
        len.write_raw_bytes_to(self);
        for xi in x {
            xi.write_raw_bytes_to(self);
        }
    }
}

/// Public methods
impl<'a> Stasher<'a> {
    /// Write a single u8 value
    pub fn u8(&mut self, x: u8) {
        self.write_primitive::<u8>(x);
    }

    /// Write a single i8 value
    pub fn i8(&mut self, x: i8) {
        self.write_primitive::<i8>(x);
    }

    /// Write a single u16 value
    pub fn u16(&mut self, x: u16) {
        self.write_primitive::<u16>(x);
    }

    /// Write a single i16 value
    pub fn i16(&mut self, x: i16) {
        self.write_primitive::<i16>(x);
    }

    /// Write a single u32 value
    pub fn u32(&mut self, x: u32) {
        self.write_primitive::<u32>(x);
    }

    /// Write a single i32 value
    pub fn i32(&mut self, x: i32) {
        self.write_primitive::<i32>(x);
    }

    /// Write a single u64 value
    pub fn u64(&mut self, x: u64) {
        self.write_primitive::<u64>(x);
    }

    /// Write a single i64 value
    pub fn i64(&mut self, x: i64) {
        self.write_primitive::<i64>(x);
    }

    /// Write a single f32 value
    pub fn f32(&mut self, x: f32) {
        self.write_primitive::<f32>(x);
    }

    /// Write a single f64 value
    pub fn f64(&mut self, x: f64) {
        self.write_primitive::<f64>(x);
    }

    /// Write an array of u8 values from a slice
    pub fn array_slice_u8(&mut self, x: &[u8]) {
        self.write_primitive_array_slice::<u8>(x);
    }

    /// Write an array of i8 values from a slice
    pub fn array_slice_i8(&mut self, x: &[i8]) {
        self.write_primitive_array_slice::<i8>(x);
    }

    /// Write an array of u16 values from a slice
    pub fn array_slice_u16(&mut self, x: &[u16]) {
        self.write_primitive_array_slice::<u16>(x);
    }

    /// Write an array of i16 values from a slice
    pub fn array_slice_i16(&mut self, x: &[i16]) {
        self.write_primitive_array_slice::<i16>(x);
    }

    /// Write an array of u32 values from a slice
    pub fn array_slice_u32(&mut self, x: &[u32]) {
        self.write_primitive_array_slice::<u32>(x);
    }

    /// Write an array of i32 values from a slice
    pub fn array_slice_i32(&mut self, x: &[i32]) {
        self.write_primitive_array_slice::<i32>(x);
    }

    /// Write an array of u64 values from a slice
    pub fn array_slice_u64(&mut self, x: &[u64]) {
        self.write_primitive_array_slice::<u64>(x);
    }

    /// Write an array of i64 values from a slice
    pub fn array_slice_i64(&mut self, x: &[i64]) {
        self.write_primitive_array_slice::<i64>(x);
    }

    /// Write an array of f32 values from a slice
    pub fn array_slice_f32(&mut self, x: &[f32]) {
        self.write_primitive_array_slice::<f32>(x);
    }

    /// Write an array of f64 values from a slice
    pub fn array_slice_f64(&mut self, x: &[f64]) {
        self.write_primitive_array_slice::<f64>(x);
    }

    // TODO: iterator support, needs some way to indicate
    // length prefix without consuming iterator just to count it

    /// Write a string
    pub fn string(&mut self, x: &str) {
        let bytes = x.as_bytes();
        self.write_raw_bytes(&[ValueType::String.to_byte()]);
        let len = bytes.len() as u32;
        len.write_raw_bytes_to(self);
        self.write_raw_bytes(bytes);
    }

    pub fn stashable<T: 'static + Stashable>(&mut self, object: &T) {
        self.write_raw_bytes(&[ValueType::StashedObject.to_byte()]);
        let hash = self.backend.stash_dependency(object);
        hash.0.write_raw_bytes_to(self);
    }
}
