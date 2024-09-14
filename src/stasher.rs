use std::hash::Hasher;

use crate::{valuetypes::PrimitiveReadWrite, StashMap, Stashable, ObjectHash, ValueType};

struct HashingStasher<'a> {
    hasher: &'a mut seahash::SeaHasher,
}

struct SerializingStasher<'a> {
    data: &'a mut Vec<u8>,
    dependencies: &'a mut Vec<ObjectHash>,
    stashmap: &'a mut StashMap,
}

enum StasherBackend<'a> {
    Hash(HashingStasher<'a>),
    Serialize(SerializingStasher<'a>),
}

impl<'a> StasherBackend<'a> {
    fn write_raw_bytes(&mut self, bytes: &[u8]) {
        match self {
            StasherBackend::Hash(hash) => {
                hash.hasher.write(bytes);
            }
            StasherBackend::Serialize(serialize) => serialize.data.extend_from_slice(bytes),
        }
    }

    fn stash_dependency<T: 'static + Stashable>(&mut self, object: &T) {
        match self {
            StasherBackend::Hash(hasher) => {
                let object_hash = ObjectHash::hash_object(object);
                hasher.hasher.write_u64(object_hash.0);
            }
            StasherBackend::Serialize(serializer) => {
                let hash = serializer.stashmap.stash_and_add_reference(object);
                serializer.dependencies.push(hash);
            }
        }
    }

    fn bookmark_length_prefix(&mut self) -> usize {
        match self {
            StasherBackend::Hash(_) => usize::MAX,
            StasherBackend::Serialize(serializer) => {
                let bookmark = serializer.data.len();
                let placeholder_length: u32 = 0;
                for b in placeholder_length.to_be_bytes() {
                    serializer.data.push(b);
                }
                bookmark
            }
        }
    }

    fn write_length_prefix(&mut self, bookmark: usize, length: u32) {
        match self {
            StasherBackend::Hash(hasher) => hasher.hasher.write_u32(length),
            StasherBackend::Serialize(serializer) => {
                for (i, b) in length.to_be_bytes().into_iter().enumerate() {
                    serializer.data[bookmark + i] = b;
                }
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
        dependencies: &'a mut Vec<ObjectHash>,
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
    fn write_primitive<T: PrimitiveReadWrite>(&mut self, x: T) {
        self.write_raw_bytes(&[ValueType::Primitive(T::TYPE).to_byte()]);
        x.write_raw_bytes_to(self);
    }

    /// Helper method to write a slice of primitives
    fn write_primitive_array<T: PrimitiveReadWrite, I: Iterator<Item = T>>(&mut self, it: I) {
        self.backend
            .write_raw_bytes(&[ValueType::Array(T::TYPE).to_byte()]);
        let bookmark = self.backend.bookmark_length_prefix();
        let mut length: u32 = 0;
        for x in it {
            x.write_raw_bytes_to(self);
            length += 1;
        }
        self.backend.write_length_prefix(bookmark, length);
    }
}

/// Public methods
impl<'a> Stasher<'a> {
    /// Write a single bool value
    pub fn bool(&mut self, x: bool) {
        self.write_primitive::<bool>(x);
    }

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
    pub fn array_of_u8_slice(&mut self, x: &[u8]) {
        self.write_primitive_array(x.iter().cloned());
    }

    /// Write an array of i8 values from a slice
    pub fn array_of_i8_slice(&mut self, x: &[i8]) {
        self.write_primitive_array(x.iter().cloned());
    }

    /// Write an array of u16 values from a slice
    pub fn array_of_u16_slice(&mut self, x: &[u16]) {
        self.write_primitive_array(x.iter().cloned());
    }

    /// Write an array of i16 values from a slice
    pub fn array_of_i16_slice(&mut self, x: &[i16]) {
        self.write_primitive_array(x.iter().cloned());
    }

    /// Write an array of u32 values from a slice
    pub fn array_of_u32_slice(&mut self, x: &[u32]) {
        self.write_primitive_array(x.iter().cloned());
    }

    /// Write an array of i32 values from a slice
    pub fn array_of_i32_slice(&mut self, x: &[i32]) {
        self.write_primitive_array(x.iter().cloned());
    }

    /// Write an array of u64 values from a slice
    pub fn array_of_u64_slice(&mut self, x: &[u64]) {
        self.write_primitive_array(x.iter().cloned());
    }

    /// Write an array of i64 values from a slice
    pub fn array_of_i64_slice(&mut self, x: &[i64]) {
        self.write_primitive_array(x.iter().cloned());
    }

    /// Write an array of f32 values from a slice
    pub fn array_of_f32_slice(&mut self, x: &[f32]) {
        self.write_primitive_array(x.iter().cloned());
    }

    /// Write an array of f64 values from a slice
    pub fn array_of_f64_slice(&mut self, x: &[f64]) {
        self.write_primitive_array(x.iter().cloned());
    }

    /// Write an array of u8 values from an iterator
    pub fn array_of_u8_iter<I: Iterator<Item = u8>>(&mut self, it: I) {
        self.write_primitive_array(it);
    }

    /// Write an array of i8 values from an iterator
    pub fn array_of_i8_iter<I: Iterator<Item = i8>>(&mut self, it: I) {
        self.write_primitive_array(it);
    }

    /// Write an array of u16 values from an iterator
    pub fn array_of_u16_iter<I: Iterator<Item = u16>>(&mut self, it: I) {
        self.write_primitive_array(it);
    }

    /// Write an array of i16 values from an iterator
    pub fn array_of_i16_iter<I: Iterator<Item = i16>>(&mut self, it: I) {
        self.write_primitive_array(it);
    }

    /// Write an array of u32 values from an iterator
    pub fn array_of_u32_iter<I: Iterator<Item = u32>>(&mut self, it: I) {
        self.write_primitive_array(it);
    }

    /// Write an array of i32 values from an iterator
    pub fn array_of_i32_iter<I: Iterator<Item = i32>>(&mut self, it: I) {
        self.write_primitive_array(it);
    }

    /// Write an array of u64 values from an iterator
    pub fn array_of_u64_iter<I: Iterator<Item = u64>>(&mut self, it: I) {
        self.write_primitive_array(it);
    }

    /// Write an array of i64 values from an iterator
    pub fn array_of_i64_iter<I: Iterator<Item = i64>>(&mut self, it: I) {
        self.write_primitive_array(it);
    }

    /// Write an array of f32 values from an iterator
    pub fn array_of_f32_iter<I: Iterator<Item = f32>>(&mut self, it: I) {
        self.write_primitive_array(it);
    }

    /// Write an array of f64 values from an iterator
    pub fn array_of_f64_iter<I: Iterator<Item = f64>>(&mut self, it: I) {
        self.write_primitive_array(it);
    }

    pub fn array_of_objects_slice<T: 'static + Stashable>(&mut self, objects: &[T]) {
        self.array_of_objects_iter(objects.iter());
    }

    pub fn array_of_objects_iter<'b, T: 'static + Stashable, I: Iterator<Item = &'b T>>(
        &mut self,
        it: I,
    ) {
        self.backend
            .write_raw_bytes(&[ValueType::ArrayOfObjects.to_byte()]);
        let bookmark = self.backend.bookmark_length_prefix();
        let mut length: u32 = 0;
        for object in it {
            self.backend.stash_dependency(object);
            length += 1;
        }
        self.backend.write_length_prefix(bookmark, length);
    }

    /// Write a string
    pub fn string(&mut self, x: &str) {
        self.backend.write_raw_bytes(&[ValueType::String.to_byte()]);
        let bookmark = self.backend.bookmark_length_prefix();
        let bytes = x.as_bytes();
        self.write_raw_bytes(bytes);
        self.backend
            .write_length_prefix(bookmark, bytes.len() as u32);
    }

    pub fn stashable<T: 'static + Stashable>(&mut self, object: &T) {
        self.write_raw_bytes(&[ValueType::StashedObject.to_byte()]);
        self.backend.stash_dependency(object);
    }
}
