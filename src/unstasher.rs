use crate::{
    valuetypes::PrimitiveReadWrite, StashMap, StashedObject, TypeSaltedHash, Unstashable,
    UnstashableInplace, ValueType,
};

pub struct Unstasher<'a> {
    bytes: &'a [u8],
    dependencies: &'a [TypeSaltedHash],
    stashmap: &'a StashMap,
}

/// Private methods
impl<'a> Unstasher<'a> {
    pub(crate) fn from_stashed_object(
        stashed_object: &'a StashedObject,
        stashmap: &'a StashMap,
    ) -> Unstasher<'a> {
        Unstasher {
            bytes: &stashed_object.bytes,
            dependencies: &stashed_object.dependencies,
            stashmap,
        }
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.bytes.is_empty() && self.dependencies.is_empty()
    }

    pub(crate) fn read_raw_bytes(&mut self, len: usize) -> Option<&[u8]> {
        if let Some((head, rest)) = self.bytes.split_at_checked(len) {
            self.bytes = rest;
            Some(head)
        } else {
            None
        }
    }

    pub(crate) fn read_raw_bytes_fixed_len<const N: usize>(&mut self) -> Option<&[u8; N]> {
        if let Some((head, rest)) = self.bytes.split_first_chunk::<N>() {
            self.bytes = rest;
            Some(head)
        } else {
            None
        }
    }

    /// Get the number of bytes that have yet to be read
    fn remaining_len(&self) -> usize {
        self.bytes.len()
    }

    /// Read the next byte and advance past it
    pub(crate) fn read_byte(&mut self) -> Result<u8, ()> {
        if let Some((head, rest)) = self.bytes.split_first() {
            let b = *head;
            self.bytes = rest;
            Ok(b)
        } else {
            Err(())
        }
    }

    /// Read the next byte without advancing past it
    fn peek_byte(&self) -> Result<u8, ()> {
        self.bytes.first().cloned().ok_or(())
    }

    fn peek_bytes(&self, len: usize) -> Result<&[u8], ()> {
        if let Some((head, _)) = self.bytes.split_at_checked(len) {
            Ok(head)
        } else {
            Err(())
        }
    }

    /// Try to perform an operation, get its result, and
    /// rollback the position in the underlying byte vector
    /// if it failed.
    fn reset_on_error<T: 'a, F: FnOnce(&mut Unstasher<'a>) -> Result<T, ()>>(
        &mut self,
        f: F,
    ) -> Result<T, ()> {
        let original_bytes = self.bytes;
        let result = f(self);
        if result.is_err() {
            self.bytes = original_bytes;
        }
        result
    }

    /// Read a single primitive, checking for its type tag first and then
    /// reading its value
    fn read_primitive<T: PrimitiveReadWrite + 'static>(&mut self) -> Result<T, ()> {
        self.reset_on_error(|unstasher| {
            if unstasher.remaining_len() < (u8::SIZE + T::SIZE) {
                return Err(());
            }
            let the_type = ValueType::from_byte(unstasher.read_byte()?)?;
            if the_type != ValueType::Primitive(T::TYPE) {
                return Err(());
            }
            Ok(T::read_raw_bytes_from(unstasher))
        })
    }

    /// Read an array of primitives to a vector, checking for its tag type and length
    /// first and then reading its values
    fn read_primitive_array_slice<T: PrimitiveReadWrite + 'static>(
        &mut self,
    ) -> Result<Vec<T>, ()> {
        self.reset_on_error(|unstasher| {
            if unstasher.remaining_len() < (u8::SIZE + u32::SIZE) {
                return Err(());
            }
            let the_type = ValueType::from_byte(unstasher.read_byte()?)?;
            if the_type != ValueType::Array(T::TYPE) {
                return Err(());
            }
            let len = u32::read_raw_bytes_from(unstasher) as usize;
            if unstasher.remaining_len() < (len * T::SIZE) {
                return Err(());
            }
            Ok((0..len)
                .map(|_| T::read_raw_bytes_from(unstasher))
                .collect())
        })
    }
}

/// Public methods
impl<'a> Unstasher<'a> {
    /// Read a single u8 value
    pub fn u8(&mut self) -> Result<u8, ()> {
        self.read_primitive::<u8>()
    }

    /// Read a single i8 value
    pub fn i8(&mut self) -> Result<i8, ()> {
        self.read_primitive::<i8>()
    }

    /// Read a single u16 value
    pub fn u16(&mut self) -> Result<u16, ()> {
        self.read_primitive::<u16>()
    }

    /// Read a single i16 value
    pub fn i16(&mut self) -> Result<i16, ()> {
        self.read_primitive::<i16>()
    }

    /// Read a single u32 value
    pub fn u32(&mut self) -> Result<u32, ()> {
        self.read_primitive::<u32>()
    }

    /// Read a single i32 value
    pub fn i32(&mut self) -> Result<i32, ()> {
        self.read_primitive::<i32>()
    }

    /// Read a single u64 value
    pub fn u64(&mut self) -> Result<u64, ()> {
        self.read_primitive::<u64>()
    }

    /// Read a single i64 value
    pub fn i64(&mut self) -> Result<i64, ()> {
        self.read_primitive::<i64>()
    }

    /// Read a single f32 value
    pub fn f32(&mut self) -> Result<f32, ()> {
        self.read_primitive::<f32>()
    }

    /// Read a single f64 value
    pub fn f64(&mut self) -> Result<f64, ()> {
        self.read_primitive::<f64>()
    }

    /// Read an array of u8 values into a Vec
    pub fn array_slice_u8(&mut self) -> Result<Vec<u8>, ()> {
        self.read_primitive_array_slice::<u8>()
    }

    /// Read an array of i8 values into a Vec
    pub fn array_slice_i8(&mut self) -> Result<Vec<i8>, ()> {
        self.read_primitive_array_slice::<i8>()
    }

    /// Read an array of u16 values into a Vec
    pub fn array_slice_u16(&mut self) -> Result<Vec<u16>, ()> {
        self.read_primitive_array_slice::<u16>()
    }

    /// Read an array of i16 values into a Vec
    pub fn array_slice_i16(&mut self) -> Result<Vec<i16>, ()> {
        self.read_primitive_array_slice::<i16>()
    }

    /// Read an array of u32 values into a Vec
    pub fn array_slice_u32(&mut self) -> Result<Vec<u32>, ()> {
        self.read_primitive_array_slice::<u32>()
    }

    /// Read an array of i32 values into a Vec
    pub fn array_slice_i32(&mut self) -> Result<Vec<i32>, ()> {
        self.read_primitive_array_slice::<i32>()
    }

    /// Read an array of u64 values into a Vec
    pub fn array_slice_u64(&mut self) -> Result<Vec<u64>, ()> {
        self.read_primitive_array_slice::<u64>()
    }

    /// Read an array of i64 values into a Vec
    pub fn array_slice_i64(&mut self) -> Result<Vec<i64>, ()> {
        self.read_primitive_array_slice::<i64>()
    }

    /// Read an array of f32 values into a Vec
    pub fn array_slice_f32(&mut self) -> Result<Vec<f32>, ()> {
        self.read_primitive_array_slice::<f32>()
    }

    /// Read an array of f64 values into a Vec
    pub fn array_slice_f64(&mut self) -> Result<Vec<f64>, ()> {
        self.read_primitive_array_slice::<f64>()
    }

    pub fn unstash<T: 'static + Unstashable>(&mut self) -> Result<T, ()> {
        self.reset_on_error(|unstasher| {
            if ValueType::from_byte(unstasher.read_byte()?)? != ValueType::StashedObject {
                return Err(());
            }
            if unstasher.remaining_len() < u64::SIZE {
                return Err(());
            }
            let hash = TypeSaltedHash(u64::read_raw_bytes_from(unstasher));
            unstasher.stashmap.unstash::<T>(hash)
        })
    }

    pub fn unstash_inplace<T: 'static + UnstashableInplace>(
        &mut self,
        object: &mut T,
    ) -> Result<(), ()> {
        self.reset_on_error(|unstasher| {
            if ValueType::from_byte(unstasher.read_byte()?)? != ValueType::StashedObject {
                return Err(());
            }
            if unstasher.remaining_len() < u64::SIZE {
                return Err(());
            }
            let hash = TypeSaltedHash(u64::read_raw_bytes_from(unstasher));
            unstasher.stashmap.unstash_inplace(hash, object)
        })
    }

    /// Read a string
    pub fn string(&mut self) -> Result<String, ()> {
        if self.remaining_len() < (u8::SIZE + u32::SIZE) {
            return Err(());
        }
        let the_type = ValueType::from_byte(self.read_byte()?)?;
        if the_type != ValueType::String {
            return Err(());
        }
        let len = u32::read_raw_bytes_from(self) as usize;
        let slice = self.read_raw_bytes(len).ok_or(())?;
        let str_slice = std::str::from_utf8(slice).map_err(|_| ())?;
        Ok(str_slice.to_string())
    }

    /// Read the type of the next value
    pub fn peek_type(&self) -> Result<ValueType, ()> {
        ValueType::from_byte(self.peek_byte()?)
    }

    /// If the next type is an array, string, or nested chive,
    /// get its length, in bytes
    pub fn peek_length_bytes(&self) -> Result<usize, ()> {
        let bytes = self.peek_bytes(5)?;
        let the_type = ValueType::from_byte(bytes[0])?;
        if let ValueType::Primitive(_) = the_type {
            return Err(());
        }
        Ok(u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize)
    }

    /// Returns true iff the chive contains no more data to read
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}
