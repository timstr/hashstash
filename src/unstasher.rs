use std::marker::PhantomData;

use crate::{
    valuetypes::PrimitiveReadWrite, StashMap, StashedObject, TypeSaltedHash, Unstashable,
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

pub struct UnstashIterator<'a, T> {
    data: &'a [u8],
    _phantom_data: PhantomData<T>,
}

impl<'a, T: PrimitiveReadWrite> Iterator for UnstashIterator<'a, T> {
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

impl<'a, T: PrimitiveReadWrite> ExactSizeIterator for UnstashIterator<'a, T> {
    fn len(&self) -> usize {
        debug_assert_eq!(self.data.len() % T::SIZE, 0);
        self.data.len() / T::SIZE
    }
}

pub(crate) struct UnstasherBackend<'a> {
    bytes: &'a [u8],
    dependencies: &'a [TypeSaltedHash],
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

    fn read_dependency(&mut self) -> Result<TypeSaltedHash, UnstashError> {
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
        self.reset_on_error(|unstasher| {
            if unstasher.remaining_len() < (u8::SIZE + u32::SIZE) {
                return Err(UnstashError::OutOfData);
            }
            let the_type = ValueType::from_byte(unstasher.read_byte().unwrap())?;
            if the_type != ValueType::Array(T::TYPE) {
                return Err(UnstashError::WrongValueType);
            }
            let len = u32::read_raw_bytes_from(&mut unstasher.bytes) as usize;
            if unstasher.remaining_len() < (len * T::SIZE) {
                return Err(UnstashError::Corrupted);
            }
            let mut v = Vec::with_capacity(len);
            for _ in 0..len {
                v.push(T::read_raw_bytes_from(&mut unstasher.bytes));
            }
            Ok(v)
        })
    }

    fn read_primitive_array_iter<T: 'static + PrimitiveReadWrite>(
        &mut self,
    ) -> Result<UnstashIterator<'a, T>, UnstashError> {
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
            let iterator = UnstashIterator {
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
        self.reset_on_error(|unstasher| {
            if unstasher.remaining_len() < (u8::SIZE + u32::SIZE) {
                return Err(UnstashError::OutOfData);
            }
            let the_type = ValueType::from_byte(unstasher.read_byte().unwrap())?;
            if the_type != ValueType::ArrayOfObjects {
                return Err(UnstashError::WrongValueType);
            }
            let len = u32::read_raw_bytes_from(&mut unstasher.bytes) as usize;
            if unstasher.dependencies.len() < len {
                return Err(UnstashError::Corrupted);
            }
            let mut v = Vec::with_capacity(len);
            for _ in 0..len {
                let hash = unstasher.read_dependency()?;
                v.push(self.stashmap.unstash(hash)?);
            }
            Ok(v)
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
    pub fn array_vec_u8(&mut self) -> Result<Vec<u8>, UnstashError> {
        self.backend.read_primitive_array_vec()
    }

    /// Read an array of i8 values into a Vec
    pub fn array_vec_i8(&mut self) -> Result<Vec<i8>, UnstashError> {
        self.backend.read_primitive_array_vec()
    }

    /// Read an array of u16 values into a Vec
    pub fn array_vec_u16(&mut self) -> Result<Vec<u16>, UnstashError> {
        self.backend.read_primitive_array_vec()
    }

    /// Read an array of i16 values into a Vec
    pub fn array_vec_i16(&mut self) -> Result<Vec<i16>, UnstashError> {
        self.backend.read_primitive_array_vec()
    }

    /// Read an array of u32 values into a Vec
    pub fn array_vec_u32(&mut self) -> Result<Vec<u32>, UnstashError> {
        self.backend.read_primitive_array_vec()
    }

    /// Read an array of i32 values into a Vec
    pub fn array_vec_i32(&mut self) -> Result<Vec<i32>, UnstashError> {
        self.backend.read_primitive_array_vec()
    }

    /// Read an array of u64 values into a Vec
    pub fn array_vec_u64(&mut self) -> Result<Vec<u64>, UnstashError> {
        self.backend.read_primitive_array_vec()
    }

    /// Read an array of i64 values into a Vec
    pub fn array_vec_i64(&mut self) -> Result<Vec<i64>, UnstashError> {
        self.backend.read_primitive_array_vec()
    }

    /// Read an array of f32 values into a Vec
    pub fn array_vec_f32(&mut self) -> Result<Vec<f32>, UnstashError> {
        self.backend.read_primitive_array_vec()
    }

    /// Read an array of f64 values into a Vec
    pub fn array_vec_f64(&mut self) -> Result<Vec<f64>, UnstashError> {
        self.backend.read_primitive_array_vec()
    }

    /// Read an array of i8 values via an iterator
    pub fn array_iter_i8(&mut self) -> Result<UnstashIterator<i8>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of u8 values via an iterator
    pub fn array_iter_u8(&mut self) -> Result<UnstashIterator<u8>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of i16 values via an iterator
    pub fn array_iter_i16(&mut self) -> Result<UnstashIterator<i16>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of u16 values via an iterator
    pub fn array_iter_u16(&mut self) -> Result<UnstashIterator<u16>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of i32 values via an iterator
    pub fn array_iter_i32(&mut self) -> Result<UnstashIterator<i32>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of u32 values via an iterator
    pub fn array_iter_u32(&mut self) -> Result<UnstashIterator<u32>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of i64 values via an iterator
    pub fn array_iter_i64(&mut self) -> Result<UnstashIterator<i64>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of u64 values via an iterator
    pub fn array_iter_u64(&mut self) -> Result<UnstashIterator<u64>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of f32 values via an iterator
    pub fn array_iter_f32(&mut self) -> Result<UnstashIterator<f32>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of f64 values via an iterator
    pub fn array_iter_f64(&mut self) -> Result<UnstashIterator<f64>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    pub fn array_of_objects_vec<T: 'static + Unstashable>(
        &mut self,
    ) -> Result<Vec<T>, UnstashError> {
        self.backend.read_array_of_object_vec()
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
    pub fn array_vec_u8(&mut self, x: &mut Vec<u8>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec(x)
    }

    /// Read an array of i8 values into a Vec
    pub fn array_vec_i8(&mut self, x: &mut Vec<i8>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec(x)
    }

    /// Read an array of u16 values into a Vec
    pub fn array_vec_u16(&mut self, x: &mut Vec<u16>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec(x)
    }

    /// Read an array of i16 values into a Vec
    pub fn array_vec_i16(&mut self, x: &mut Vec<i16>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec(x)
    }

    /// Read an array of u32 values into a Vec
    pub fn array_vec_u32(&mut self, x: &mut Vec<u32>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec(x)
    }

    /// Read an array of i32 values into a Vec
    pub fn array_vec_i32(&mut self, x: &mut Vec<i32>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec(x)
    }

    /// Read an array of u64 values into a Vec
    pub fn array_vec_u64(&mut self, x: &mut Vec<u64>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec(x)
    }

    /// Read an array of i64 values into a Vec
    pub fn array_vec_i64(&mut self, x: &mut Vec<i64>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec(x)
    }

    /// Read an array of f32 values into a Vec
    pub fn array_vec_f32(&mut self, x: &mut Vec<f32>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec(x)
    }

    /// Read an array of f64 values into a Vec
    pub fn array_vec_f64(&mut self, x: &mut Vec<f64>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec(x)
    }

    // TODO: is there any way to do two-phase in-place unstashing with iterators of unknown count?

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
