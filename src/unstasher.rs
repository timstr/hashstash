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

    pub(crate) fn read_raw_bytes_fixed_len<const N: usize>(
        &mut self,
    ) -> Result<&[u8; N], UnstashError> {
        if let Some((head, rest)) = self.bytes.split_first_chunk::<N>() {
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
    fn read_primitive<T: PrimitiveReadWrite + 'static>(&mut self) -> Result<T, UnstashError> {
        self.reset_on_error(|unstasher| {
            if unstasher.remaining_len() < (u8::SIZE + T::SIZE) {
                return Err(UnstashError::OutOfData);
            }
            let the_type = ValueType::from_byte(unstasher.read_byte().unwrap())?;
            if the_type != ValueType::Primitive(T::TYPE) {
                return Err(UnstashError::WrongValueType);
            }
            Ok(T::read_raw_bytes_from(unstasher))
        })
    }

    /// Read an array of primitives to a vector, checking for its tag type and length
    /// first and then reading its values
    fn read_primitive_array_slice<T: PrimitiveReadWrite + 'static>(
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
            let len = u32::read_raw_bytes_from(unstasher) as usize;
            if unstasher.remaining_len() < (len * T::SIZE) {
                return Err(UnstashError::Corrupted);
            }
            Ok((0..len)
                .map(|_| T::read_raw_bytes_from(unstasher))
                .collect())
        })
    }
}

impl<'a> UnstasherBackend<'a> {
    fn unstash<T: 'static + Unstashable>(&mut self) -> Result<T, UnstashError> {
        self.reset_on_error(|unstasher| {
            if ValueType::from_byte(unstasher.read_byte()?)? != ValueType::StashedObject {
                return Err(UnstashError::WrongValueType);
            }
            let Some((hash, remaining_hashes)) = unstasher.dependencies.split_first() else {
                return Err(UnstashError::Corrupted);
            };
            unstasher.dependencies = remaining_hashes;
            unstasher.stashmap.unstash::<T>(*hash)
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
            let Some((hash, remaining_hashes)) = unstasher.dependencies.split_first() else {
                return Err(UnstashError::Corrupted);
            };
            unstasher.dependencies = remaining_hashes;
            unstasher.stashmap.unstash_inplace(*hash, object, phase)
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
        let len = u32::read_raw_bytes_from(self) as usize;
        let slice = self.read_raw_bytes(len)?;
        let str_slice = std::str::from_utf8(slice).map_err(|_| UnstashError::Corrupted)?;
        Ok(str_slice.to_string())
    }

    /// Read the type of the next value
    fn peek_type(&self) -> Result<ValueType, UnstashError> {
        ValueType::from_byte(self.peek_byte()?)
    }

    /// If the next type is an array or string, get its length in bytes
    fn peek_length_bytes(&self) -> Result<usize, UnstashError> {
        let bytes = self.peek_bytes(5)?;
        let the_type = ValueType::from_byte(bytes[0])?;
        match the_type {
            ValueType::Array(_) => (),
            ValueType::String => (),
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
        self.backend.read_primitive::<u8>()
    }

    /// Read a single i8 value
    pub fn i8(&mut self) -> Result<i8, UnstashError> {
        self.backend.read_primitive::<i8>()
    }

    /// Read a single u16 value
    pub fn u16(&mut self) -> Result<u16, UnstashError> {
        self.backend.read_primitive::<u16>()
    }

    /// Read a single i16 value
    pub fn i16(&mut self) -> Result<i16, UnstashError> {
        self.backend.read_primitive::<i16>()
    }

    /// Read a single u32 value
    pub fn u32(&mut self) -> Result<u32, UnstashError> {
        self.backend.read_primitive::<u32>()
    }

    /// Read a single i32 value
    pub fn i32(&mut self) -> Result<i32, UnstashError> {
        self.backend.read_primitive::<i32>()
    }

    /// Read a single u64 value
    pub fn u64(&mut self) -> Result<u64, UnstashError> {
        self.backend.read_primitive::<u64>()
    }

    /// Read a single i64 value
    pub fn i64(&mut self) -> Result<i64, UnstashError> {
        self.backend.read_primitive::<i64>()
    }

    /// Read a single f32 value
    pub fn f32(&mut self) -> Result<f32, UnstashError> {
        self.backend.read_primitive::<f32>()
    }

    /// Read a single f64 value
    pub fn f64(&mut self) -> Result<f64, UnstashError> {
        self.backend.read_primitive::<f64>()
    }

    /// Read an array of u8 values into a Vec
    pub fn array_slice_u8(&mut self) -> Result<Vec<u8>, UnstashError> {
        self.backend.read_primitive_array_slice::<u8>()
    }

    /// Read an array of i8 values into a Vec
    pub fn array_slice_i8(&mut self) -> Result<Vec<i8>, UnstashError> {
        self.backend.read_primitive_array_slice::<i8>()
    }

    /// Read an array of u16 values into a Vec
    pub fn array_slice_u16(&mut self) -> Result<Vec<u16>, UnstashError> {
        self.backend.read_primitive_array_slice::<u16>()
    }

    /// Read an array of i16 values into a Vec
    pub fn array_slice_i16(&mut self) -> Result<Vec<i16>, UnstashError> {
        self.backend.read_primitive_array_slice::<i16>()
    }

    /// Read an array of u32 values into a Vec
    pub fn array_slice_u32(&mut self) -> Result<Vec<u32>, UnstashError> {
        self.backend.read_primitive_array_slice::<u32>()
    }

    /// Read an array of i32 values into a Vec
    pub fn array_slice_i32(&mut self) -> Result<Vec<i32>, UnstashError> {
        self.backend.read_primitive_array_slice::<i32>()
    }

    /// Read an array of u64 values into a Vec
    pub fn array_slice_u64(&mut self) -> Result<Vec<u64>, UnstashError> {
        self.backend.read_primitive_array_slice::<u64>()
    }

    /// Read an array of i64 values into a Vec
    pub fn array_slice_i64(&mut self) -> Result<Vec<i64>, UnstashError> {
        self.backend.read_primitive_array_slice::<i64>()
    }

    /// Read an array of f32 values into a Vec
    pub fn array_slice_f32(&mut self) -> Result<Vec<f32>, UnstashError> {
        self.backend.read_primitive_array_slice::<f32>()
    }

    /// Read an array of f64 values into a Vec
    pub fn array_slice_f64(&mut self) -> Result<Vec<f64>, UnstashError> {
        self.backend.read_primitive_array_slice::<f64>()
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

    pub fn peek_length_bytes(&self) -> Result<usize, UnstashError> {
        self.backend.peek_length_bytes()
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
    pub fn array_slice_u8(&mut self, x: &mut Vec<u8>) -> Result<(), UnstashError> {
        *x = self.backend.read_primitive_array_slice::<u8>()?;
        Ok(())
    }

    /// Read an array of i8 values into a Vec
    pub fn array_slice_i8(&mut self, x: &mut Vec<i8>) -> Result<(), UnstashError> {
        *x = self.backend.read_primitive_array_slice::<i8>()?;
        Ok(())
    }

    /// Read an array of u16 values into a Vec
    pub fn array_slice_u16(&mut self, x: &mut Vec<u16>) -> Result<(), UnstashError> {
        *x = self.backend.read_primitive_array_slice::<u16>()?;
        Ok(())
    }

    /// Read an array of i16 values into a Vec
    pub fn array_slice_i16(&mut self, x: &mut Vec<i16>) -> Result<(), UnstashError> {
        *x = self.backend.read_primitive_array_slice::<i16>()?;
        Ok(())
    }

    /// Read an array of u32 values into a Vec
    pub fn array_slice_u32(&mut self, x: &mut Vec<u32>) -> Result<(), UnstashError> {
        *x = self.backend.read_primitive_array_slice::<u32>()?;
        Ok(())
    }

    /// Read an array of i32 values into a Vec
    pub fn array_slice_i32(&mut self, x: &mut Vec<i32>) -> Result<(), UnstashError> {
        *x = self.backend.read_primitive_array_slice::<i32>()?;
        Ok(())
    }

    /// Read an array of u64 values into a Vec
    pub fn array_slice_u64(&mut self, x: &mut Vec<u64>) -> Result<(), UnstashError> {
        *x = self.backend.read_primitive_array_slice::<u64>()?;
        Ok(())
    }

    /// Read an array of i64 values into a Vec
    pub fn array_slice_i64(&mut self, x: &mut Vec<i64>) -> Result<(), UnstashError> {
        *x = self.backend.read_primitive_array_slice::<i64>()?;
        Ok(())
    }

    /// Read an array of f32 values into a Vec
    pub fn array_slice_f32(&mut self, x: &mut Vec<f32>) -> Result<(), UnstashError> {
        *x = self.backend.read_primitive_array_slice::<f32>()?;
        Ok(())
    }

    /// Read an array of f64 values into a Vec
    pub fn array_slice_f64(&mut self, x: &mut Vec<f64>) -> Result<(), UnstashError> {
        *x = self.backend.read_primitive_array_slice::<f64>()?;
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

    pub fn peek_length_bytes(&self) -> Result<usize, UnstashError> {
        self.backend.peek_length_bytes()
    }

    pub fn is_empty(&self) -> bool {
        self.backend.is_empty()
    }
}
