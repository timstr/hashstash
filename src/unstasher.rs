use std::marker::PhantomData;

use crate::{
    valuetypes::PrimitiveReadWrite, ObjectHash, StashMap, StashedObject, Unstashable,
    UnstashableInplace, ValueType,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnstashError {
    WrongValueType,
    OutOfData,
    Corrupted,
    NotFinished,

    // TODO: NotFound should probably be merged with Corrupted
    NotFound,
}

pub struct PrimitiveIterator<'a, T> {
    data: &'a [u8],
    _phantom_data: PhantomData<T>,
}

impl<'a, T: PrimitiveReadWrite> Iterator for PrimitiveIterator<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        debug_assert_eq!(self.data.len() % T::SIZE, 0);
        if !self.data.is_empty() {
            Some(T::read_raw_bytes_from(&mut self.data))
        } else {
            None
        }
    }
}

impl<'a, T: PrimitiveReadWrite> ExactSizeIterator for PrimitiveIterator<'a, T> {
    fn len(&self) -> usize {
        debug_assert_eq!(self.data.len() % T::SIZE, 0);
        self.data.len() / T::SIZE
    }
}

pub struct ObjectIterator<'a, T> {
    hashes: &'a [ObjectHash],
    stashmap: &'a StashMap,
    _phantom_data: PhantomData<T>,
}

impl<'a, T: Unstashable> Iterator for ObjectIterator<'a, T> {
    type Item = Result<T, UnstashError>;

    fn next(&mut self) -> Option<Self::Item> {
        let Some((hash, remaining_hashes)) = self.hashes.split_first() else {
            return None;
        };
        self.hashes = remaining_hashes;
        Some(self.stashmap.unstash(*hash))
    }
}

pub(crate) struct UnstasherBackend<'a> {
    bytes: &'a [u8],
    dependencies: &'a [ObjectHash],
    stashmap: &'a StashMap,
}

/// Private methods
impl<'a> UnstasherBackend<'a> {
    pub(crate) fn from_stashed_object(
        stashed_object: &'a StashedObject,
        stashmap: &'a StashMap,
    ) -> UnstasherBackend<'a> {
        UnstasherBackend {
            bytes: &stashed_object.bytes,
            dependencies: &stashed_object.dependencies,
            stashmap,
        }
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.bytes.is_empty() && self.dependencies.is_empty()
    }

    pub(crate) fn read_raw_bytes(&mut self, len: usize) -> Result<&[u8], UnstashError> {
        if let Some((head, rest)) = self.bytes.split_at_checked(len) {
            self.bytes = rest;
            Ok(head)
        } else {
            Err(UnstashError::OutOfData)
        }
    }

    /// Get the number of bytes that have yet to be read
    fn remaining_len(&self) -> usize {
        self.bytes.len()
    }

    /// Read the next byte and advance past it
    pub(crate) fn read_byte(&mut self) -> Result<u8, UnstashError> {
        if let Some((head, rest)) = self.bytes.split_first() {
            let b = *head;
            self.bytes = rest;
            Ok(b)
        } else {
            Err(UnstashError::OutOfData)
        }
    }

    /// Read the next byte without advancing past it
    fn peek_byte(&self) -> Result<u8, UnstashError> {
        self.bytes.first().cloned().ok_or(UnstashError::OutOfData)
    }

    fn peek_bytes(&self, len: usize) -> Result<&[u8], UnstashError> {
        if let Some((head, _)) = self.bytes.split_at_checked(len) {
            Ok(head)
        } else {
            Err(UnstashError::OutOfData)
        }
    }

    fn read_dependency(&mut self) -> Result<ObjectHash, UnstashError> {
        let Some((hash, remaining_hashes)) = self.dependencies.split_first() else {
            return Err(UnstashError::Corrupted);
        };
        self.dependencies = remaining_hashes;
        Ok(*hash)
    }

    /// Try to perform an operation, get its result, and
    /// rollback the position in the underlying byte vector
    /// if it failed.
    fn reset_on_error<T: 'a, F: FnOnce(&mut UnstasherBackend<'a>) -> Result<T, UnstashError>>(
        &mut self,
        f: F,
    ) -> Result<T, UnstashError> {
        let original_bytes = self.bytes;
        let result = f(self);
        if result.is_err() {
            self.bytes = original_bytes;
        }
        result
    }

    /// Read a single primitive, checking for its type tag first and then
    /// reading its value
    fn read_primitive<T: 'static + PrimitiveReadWrite>(&mut self) -> Result<T, UnstashError> {
        self.reset_on_error(|unstasher| {
            if unstasher.remaining_len() < (1 + T::SIZE) {
                return Err(UnstashError::OutOfData);
            }
            let the_type = ValueType::from_byte(unstasher.read_byte().unwrap())?;
            if the_type != ValueType::Primitive(T::TYPE) {
                return Err(UnstashError::WrongValueType);
            }
            let x = T::read_raw_bytes_from(&mut unstasher.bytes);
            Ok(x)
        })
    }

    /// Read an array of primitives to a vector, checking for its tag type and length
    /// first and then reading its values
    fn read_primitive_array_vec<T: 'static + PrimitiveReadWrite>(
        &mut self,
    ) -> Result<Vec<T>, UnstashError> {
        Ok(self.read_primitive_array_iter()?.collect())
    }

    fn read_primitive_array_iter<T: 'static + PrimitiveReadWrite>(
        &mut self,
    ) -> Result<PrimitiveIterator<'a, T>, UnstashError> {
        self.reset_on_error(|unstasher| {
            if unstasher.remaining_len() < (u8::SIZE + u32::SIZE) {
                return Err(UnstashError::OutOfData);
            }
            let the_type = ValueType::from_byte(unstasher.read_byte().unwrap())?;
            if the_type != ValueType::Array(T::TYPE) {
                return Err(UnstashError::WrongValueType);
            }
            let len = u32::read_raw_bytes_from(&mut unstasher.bytes) as usize;
            let num_bytes = len * T::SIZE;
            if unstasher.remaining_len() < num_bytes {
                return Err(UnstashError::Corrupted);
            }
            let iterator = PrimitiveIterator {
                data: &unstasher.bytes[..num_bytes],
                _phantom_data: PhantomData,
            };
            unstasher.bytes = &unstasher.bytes[num_bytes..];
            Ok(iterator)
        })
    }

    fn read_array_of_object_vec<T: 'static + Unstashable>(
        &mut self,
    ) -> Result<Vec<T>, UnstashError> {
        self.read_array_of_object_iter()?.collect()
    }

    fn read_array_of_object_iter<T: 'static + Unstashable>(
        &mut self,
    ) -> Result<ObjectIterator<T>, UnstashError> {
        self.reset_on_error(|unstasher| {
            if unstasher.remaining_len() < (u8::SIZE + u32::SIZE) {
                return Err(UnstashError::OutOfData);
            }
            let the_type = ValueType::from_byte(unstasher.read_byte().unwrap())?;
            if the_type != ValueType::ArrayOfObjects {
                return Err(UnstashError::WrongValueType);
            }
            let len = u32::read_raw_bytes_from(&mut unstasher.bytes) as usize;
            let Some((hashes, remaining_hashes)) = unstasher.dependencies.split_at_checked(len)
            else {
                return Err(UnstashError::Corrupted);
            };
            unstasher.dependencies = remaining_hashes;
            let iter = ObjectIterator {
                hashes,
                stashmap: unstasher.stashmap,
                _phantom_data: PhantomData,
            };
            Ok(iter)
        })
    }

    fn unstash<T: 'static + Unstashable>(&mut self) -> Result<T, UnstashError> {
        self.reset_on_error(|unstasher| {
            if ValueType::from_byte(unstasher.read_byte()?)? != ValueType::StashedObject {
                return Err(UnstashError::WrongValueType);
            }
            let hash = unstasher.read_dependency()?;
            unstasher.stashmap.unstash::<T>(hash)
        })
    }

    fn unstash_inplace<T: 'static + UnstashableInplace>(
        &mut self,
        object: &mut T,
        phase: InplaceUnstashPhase,
    ) -> Result<(), UnstashError> {
        self.reset_on_error(|unstasher| {
            if ValueType::from_byte(unstasher.read_byte()?)? != ValueType::StashedObject {
                return Err(UnstashError::WrongValueType);
            }
            let hash = unstasher.read_dependency()?;
            unstasher.stashmap.unstash_inplace(hash, object, phase)
        })
    }

    fn string(&mut self) -> Result<String, UnstashError> {
        if self.remaining_len() < (u8::SIZE + u32::SIZE) {
            return Err(UnstashError::OutOfData);
        }
        let the_type = ValueType::from_byte(self.read_byte()?)?;
        if the_type != ValueType::String {
            return Err(UnstashError::WrongValueType);
        }
        let len = u32::read_raw_bytes_from(&mut self.bytes) as usize;
        let slice = self.read_raw_bytes(len)?;
        let str_slice = std::str::from_utf8(slice).map_err(|_| UnstashError::Corrupted)?;
        Ok(str_slice.to_string())
    }

    /// Read the type of the next value
    fn peek_type(&self) -> Result<ValueType, UnstashError> {
        ValueType::from_byte(self.peek_byte()?)
    }

    /// If the next type is an array, get the number of items
    /// If the next type is a string, get its length in bytes
    fn peek_length(&self) -> Result<usize, UnstashError> {
        let bytes = self.peek_bytes(5)?;
        let the_type = ValueType::from_byte(bytes[0])?;
        match the_type {
            ValueType::Array(_) => (),
            ValueType::String => (),
            ValueType::ArrayOfObjects => (),
            _ => return Err(UnstashError::WrongValueType),
        }
        Ok(u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize)
    }

    /// Returns true iff there is no more data to read
    fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

pub struct Unstasher<'a> {
    backend: UnstasherBackend<'a>,
}

impl<'a> Unstasher<'a> {
    pub(crate) fn new(backend: UnstasherBackend<'a>) -> Unstasher<'a> {
        Unstasher { backend }
    }

    pub(crate) fn backend(&self) -> &UnstasherBackend<'a> {
        &self.backend
    }
}

impl<'a> Unstasher<'a> {
    /// Read a single bool value
    pub fn bool(&mut self) -> Result<bool, UnstashError> {
        self.backend.read_primitive::<bool>()
    }

    /// Read a single u8 value
    pub fn u8(&mut self) -> Result<u8, UnstashError> {
        self.backend.read_primitive()
    }

    /// Read a single i8 value
    pub fn i8(&mut self) -> Result<i8, UnstashError> {
        self.backend.read_primitive()
    }

    /// Read a single u16 value
    pub fn u16(&mut self) -> Result<u16, UnstashError> {
        self.backend.read_primitive()
    }

    /// Read a single i16 value
    pub fn i16(&mut self) -> Result<i16, UnstashError> {
        self.backend.read_primitive()
    }

    /// Read a single u32 value
    pub fn u32(&mut self) -> Result<u32, UnstashError> {
        self.backend.read_primitive()
    }

    /// Read a single i32 value
    pub fn i32(&mut self) -> Result<i32, UnstashError> {
        self.backend.read_primitive()
    }

    /// Read a single u64 value
    pub fn u64(&mut self) -> Result<u64, UnstashError> {
        self.backend.read_primitive()
    }

    /// Read a single i64 value
    pub fn i64(&mut self) -> Result<i64, UnstashError> {
        self.backend.read_primitive()
    }

    /// Read a single f32 value
    pub fn f32(&mut self) -> Result<f32, UnstashError> {
        self.backend.read_primitive()
    }

    /// Read a single f64 value
    pub fn f64(&mut self) -> Result<f64, UnstashError> {
        self.backend.read_primitive()
    }

    /// Read an array of u8 values into a Vec
    pub fn array_of_u8_vec(&mut self) -> Result<Vec<u8>, UnstashError> {
        self.backend.read_primitive_array_vec()
    }

    /// Read an array of i8 values into a Vec
    pub fn array_of_i8_vec(&mut self) -> Result<Vec<i8>, UnstashError> {
        self.backend.read_primitive_array_vec()
    }

    /// Read an array of u16 values into a Vec
    pub fn array_of_u16_vec(&mut self) -> Result<Vec<u16>, UnstashError> {
        self.backend.read_primitive_array_vec()
    }

    /// Read an array of i16 values into a Vec
    pub fn array_of_i16_vec(&mut self) -> Result<Vec<i16>, UnstashError> {
        self.backend.read_primitive_array_vec()
    }

    /// Read an array of u32 values into a Vec
    pub fn array_of_u32_vec(&mut self) -> Result<Vec<u32>, UnstashError> {
        self.backend.read_primitive_array_vec()
    }

    /// Read an array of i32 values into a Vec
    pub fn array_of_i32_vec(&mut self) -> Result<Vec<i32>, UnstashError> {
        self.backend.read_primitive_array_vec()
    }

    /// Read an array of u64 values into a Vec
    pub fn array_of_u64_vec(&mut self) -> Result<Vec<u64>, UnstashError> {
        self.backend.read_primitive_array_vec()
    }

    /// Read an array of i64 values into a Vec
    pub fn array_of_i64_vec(&mut self) -> Result<Vec<i64>, UnstashError> {
        self.backend.read_primitive_array_vec()
    }

    /// Read an array of f32 values into a Vec
    pub fn array_of_f32_vec(&mut self) -> Result<Vec<f32>, UnstashError> {
        self.backend.read_primitive_array_vec()
    }

    /// Read an array of f64 values into a Vec
    pub fn array_of_f64_vec(&mut self) -> Result<Vec<f64>, UnstashError> {
        self.backend.read_primitive_array_vec()
    }

    /// Read an array of i8 values via an iterator
    pub fn array_of_i8_iter(&mut self) -> Result<PrimitiveIterator<i8>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of u8 values via an iterator
    pub fn array_of_u8_iter(&mut self) -> Result<PrimitiveIterator<u8>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of i16 values via an iterator
    pub fn array_of_i16_iter(&mut self) -> Result<PrimitiveIterator<i16>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of u16 values via an iterator
    pub fn array_of_u16_iter(&mut self) -> Result<PrimitiveIterator<u16>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of i32 values via an iterator
    pub fn array_of_i32_iter(&mut self) -> Result<PrimitiveIterator<i32>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of u32 values via an iterator
    pub fn array_of_u32_iter(&mut self) -> Result<PrimitiveIterator<u32>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of i64 values via an iterator
    pub fn array_of_i64_iter(&mut self) -> Result<PrimitiveIterator<i64>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of u64 values via an iterator
    pub fn array_of_u64_iter(&mut self) -> Result<PrimitiveIterator<u64>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of f32 values via an iterator
    pub fn array_of_f32_iter(&mut self) -> Result<PrimitiveIterator<f32>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of f64 values via an iterator
    pub fn array_of_f64_iter(&mut self) -> Result<PrimitiveIterator<f64>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    pub fn array_of_objects_vec<T: 'static + Unstashable>(
        &mut self,
    ) -> Result<Vec<T>, UnstashError> {
        self.backend.read_array_of_object_vec()
    }

    pub fn array_of_objects_iter<T: 'static + Unstashable>(
        &mut self,
    ) -> Result<ObjectIterator<T>, UnstashError> {
        self.backend.read_array_of_object_iter()
    }

    pub fn string(&mut self) -> Result<String, UnstashError> {
        self.backend.string()
    }

    pub fn unstash<T: 'static + Unstashable>(&mut self) -> Result<T, UnstashError> {
        self.backend.unstash()
    }

    pub fn peek_type(&self) -> Result<ValueType, UnstashError> {
        self.backend.peek_type()
    }

    pub fn peek_length(&self) -> Result<usize, UnstashError> {
        self.backend.peek_length()
    }

    pub fn is_empty(&self) -> bool {
        self.backend.is_empty()
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum InplaceUnstashPhase {
    Validate,
    Write,
}

pub struct InplaceUnstasher<'a> {
    backend: UnstasherBackend<'a>,
    phase: InplaceUnstashPhase,
}

impl<'a> InplaceUnstasher<'a> {
    pub(crate) fn new(
        backend: UnstasherBackend<'a>,
        phase: InplaceUnstashPhase,
    ) -> InplaceUnstasher<'a> {
        InplaceUnstasher { backend, phase }
    }

    pub(crate) fn backend(&self) -> &UnstasherBackend<'a> {
        &self.backend
    }

    fn read_primitive<T: 'static + PrimitiveReadWrite>(
        &mut self,
        x: &mut T,
    ) -> Result<(), UnstashError> {
        let y = self.backend.read_primitive::<T>()?;
        if self.phase == InplaceUnstashPhase::Write {
            *x = y;
        }
        Ok(())
    }

    fn read_primitive_array_vec<T: 'static + PrimitiveReadWrite>(
        &mut self,
        v: &mut Vec<T>,
    ) -> Result<(), UnstashError> {
        let v2 = self.backend.read_primitive_array_vec::<T>()?;
        if self.phase == InplaceUnstashPhase::Write {
            *v = v2;
        }
        Ok(())
    }
}

impl<'a> InplaceUnstasher<'a> {
    /// Read a single bool value
    pub fn bool(&mut self, x: &mut bool) -> Result<(), UnstashError> {
        self.read_primitive(x)
    }

    /// Read a single u8 value
    pub fn u8(&mut self, x: &mut u8) -> Result<(), UnstashError> {
        self.read_primitive(x)
    }

    /// Read a single i8 value
    pub fn i8(&mut self, x: &mut i8) -> Result<(), UnstashError> {
        self.read_primitive(x)
    }

    /// Read a single u16 value
    pub fn u16(&mut self, x: &mut u16) -> Result<(), UnstashError> {
        self.read_primitive(x)
    }

    /// Read a single i16 value
    pub fn i16(&mut self, x: &mut i16) -> Result<(), UnstashError> {
        self.read_primitive(x)
    }

    /// Read a single u32 value
    pub fn u32(&mut self, x: &mut u32) -> Result<(), UnstashError> {
        self.read_primitive(x)
    }

    /// Read a single i32 value
    pub fn i32(&mut self, x: &mut i32) -> Result<(), UnstashError> {
        self.read_primitive(x)
    }

    /// Read a single u64 value
    pub fn u64(&mut self, x: &mut u64) -> Result<(), UnstashError> {
        self.read_primitive(x)
    }

    /// Read a single i64 value
    pub fn i64(&mut self, x: &mut i64) -> Result<(), UnstashError> {
        self.read_primitive(x)
    }

    /// Read a single f32 value
    pub fn f32(&mut self, x: &mut f32) -> Result<(), UnstashError> {
        self.read_primitive(x)
    }

    /// Read a single f64 value
    pub fn f64(&mut self, x: &mut f64) -> Result<(), UnstashError> {
        self.read_primitive(x)
    }

    /// Read an array of u8 values into a Vec
    pub fn array_of_u8_vec(&mut self, x: &mut Vec<u8>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec(x)
    }

    /// Read an array of i8 values into a Vec
    pub fn array_of_i8_vec(&mut self, x: &mut Vec<i8>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec(x)
    }

    /// Read an array of u16 values into a Vec
    pub fn array_of_u16_vec(&mut self, x: &mut Vec<u16>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec(x)
    }

    /// Read an array of i16 values into a Vec
    pub fn array_of_i16_vec(&mut self, x: &mut Vec<i16>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec(x)
    }

    /// Read an array of u32 values into a Vec
    pub fn array_of_u32_vec(&mut self, x: &mut Vec<u32>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec(x)
    }

    /// Read an array of i32 values into a Vec
    pub fn array_of_i32_vec(&mut self, x: &mut Vec<i32>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec(x)
    }

    /// Read an array of u64 values into a Vec
    pub fn array_of_u64_vec(&mut self, x: &mut Vec<u64>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec(x)
    }

    /// Read an array of i64 values into a Vec
    pub fn array_of_i64_vec(&mut self, x: &mut Vec<i64>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec(x)
    }

    /// Read an array of f32 values into a Vec
    pub fn array_of_f32_vec(&mut self, x: &mut Vec<f32>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec(x)
    }

    /// Read an array of f64 values into a Vec
    pub fn array_of_f64_vec(&mut self, x: &mut Vec<f64>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec(x)
    }

    // TODO: is there any way to do two-phase in-place unstashing with iterators
    // of unknown count? Slice and vec are cool and useful but an iterator-based
    // interface will support way more types containers

    pub fn array_of_objects_vec<T: 'static + Unstashable>(
        &mut self,
        x: &mut Vec<T>,
    ) -> Result<(), UnstashError> {
        let v = self.backend.read_array_of_object_vec()?;
        if self.phase == InplaceUnstashPhase::Write {
            *x = v;
        }
        Ok(())
    }

    pub fn string(&mut self, x: &mut String) -> Result<(), UnstashError> {
        let s = self.backend.string()?;
        if self.phase == InplaceUnstashPhase::Write {
            *x = s;
        }
        Ok(())
    }

    pub fn unstash<T: 'static + Unstashable>(
        &mut self,
        object: &mut T,
    ) -> Result<(), UnstashError> {
        let other_object = self.backend.unstash()?;
        if self.phase == InplaceUnstashPhase::Write {
            *object = other_object;
        }
        Ok(())
    }

    pub fn unstash_inplace<T: 'static + UnstashableInplace>(
        &mut self,
        object: &mut T,
    ) -> Result<(), UnstashError> {
        self.backend.unstash_inplace(object, self.phase)
    }

    pub fn peek_type(&self) -> Result<ValueType, UnstashError> {
        self.backend.peek_type()
    }

    pub fn peek_length(&self) -> Result<usize, UnstashError> {
        self.backend.peek_length()
    }

    pub fn is_empty(&self) -> bool {
        self.backend.is_empty()
    }
}
