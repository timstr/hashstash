use std::marker::PhantomData;

use crate::{
    valuetypes::PrimitiveReadWrite, ObjectHash, StashMap, StashedObject, Unstashable,
    UnstashableInplace, ValueType,
};

/// Error that can happen while unstashing an object
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnstashError {
    /// The next stashed value does not have the expected type
    WrongValueType,

    /// There isn't any stashed data left
    OutOfData,

    /// The stashed data is internally inconsistent
    Corrupted,

    /// An object was unstashed without reading all its stashed data
    NotFinished,

    /// A value was read succesfully, but it has no valid interpretation
    /// in context. This is intended mainly for client use.
    BadValue,
}

/// Iterator over an array of primitives being unstashed
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

/// Iterator over an array of [Unstashable] objects being unstashed
pub struct ObjectIterator<'a, C, T> {
    hashes: &'a [ObjectHash],
    stashmap: &'a StashMap,
    context: C,
    _phantom_data: PhantomData<T>,
}

impl<'a, C: Copy, T: Unstashable<C>> Iterator for ObjectIterator<'a, C, T> {
    type Item = Result<T, UnstashError>;

    fn next(&mut self) -> Option<Self::Item> {
        let Some((hash, remaining_hashes)) = self.hashes.split_first() else {
            return None;
        };
        self.hashes = remaining_hashes;
        Some(self.stashmap.unstash(*hash, T::unstash, self.context))
    }
}

/// The backend for both an [Unstasher] and an [InplaceUnstasher]
#[derive(Copy, Clone)]
pub(crate) struct UnstasherBackend<'a> {
    bytes: &'a [u8],
    dependencies: &'a [ObjectHash],
    stashmap: &'a StashMap,
}

/// Private methods
impl<'a> UnstasherBackend<'a> {
    /// Create a new backend from a stashed object
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

    /// Have all serialized contents and dependencies been read?
    pub(crate) fn is_finished(&self) -> bool {
        self.bytes.is_empty() && self.dependencies.is_empty()
    }

    /// Read a sequence of raw bytes
    pub(crate) fn read_raw_bytes(&mut self, len: usize) -> Result<&[u8], UnstashError> {
        if let Some((head, rest)) = self.bytes.split_at_checked(len) {
            self.bytes = rest;
            Ok(head)
        } else {
            Err(UnstashError::Corrupted)
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

    /// Read a sequence of bytes without advancing
    fn peek_bytes(&self, len: usize) -> Result<&[u8], UnstashError> {
        if let Some((head, _)) = self.bytes.split_at_checked(len) {
            Ok(head)
        } else {
            Err(UnstashError::OutOfData)
        }
    }

    /// Read the [ValueType] at the next byte
    fn read_value_type(&mut self) -> Result<ValueType, UnstashError> {
        ValueType::from_byte(self.read_byte()?)
    }

    /// Read the 32-bit length at the next four bytes.
    /// This assumes that we are in the middle of reading
    /// a value type with a prefixed length.
    fn read_value_length(&mut self) -> Result<usize, UnstashError> {
        if self.remaining_len() < u32::SIZE {
            return Err(UnstashError::Corrupted);
        }
        let len = u32::read_raw_bytes_from(&mut self.bytes);
        Ok(len as usize)
    }

    /// Read the hash of the next dependency
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
    fn reset_on_error<
        Context: Copy,
        T,
        F: FnOnce(&mut UnstasherBackend<'a>, Context) -> Result<T, UnstashError>,
    >(
        &mut self,
        f: F,
        context: Context,
    ) -> Result<T, UnstashError> {
        let original = self.clone();
        let result = f(self, context);
        if result.is_err() {
            *self = original;
        }
        result
    }

    /// Read a single primitive, checking for its type tag first and then
    /// reading its value
    fn read_primitive<T: 'static + PrimitiveReadWrite>(&mut self) -> Result<T, UnstashError> {
        self.reset_on_error(
            |unstasher, _| {
                if unstasher.read_value_type()? != ValueType::Primitive(T::TYPE) {
                    return Err(UnstashError::WrongValueType);
                }
                let x = T::read_raw_bytes_from(&mut unstasher.bytes);
                Ok(x)
            },
            (),
        )
    }

    /// Read an array of primitives to a vector
    fn read_primitive_array_vec<T: 'static + PrimitiveReadWrite>(
        &mut self,
    ) -> Result<Vec<T>, UnstashError> {
        Ok(self.read_primitive_array_iter()?.collect())
    }

    /// Read an array of primitives via an iterator
    fn read_primitive_array_iter<T: 'static + PrimitiveReadWrite>(
        &mut self,
    ) -> Result<PrimitiveIterator<'a, T>, UnstashError> {
        self.reset_on_error(
            |unstasher, _| {
                if unstasher.read_value_type()? != ValueType::Array(T::TYPE) {
                    return Err(UnstashError::WrongValueType);
                }
                let len = unstasher.read_value_length()?;
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
            },
            (),
        )
    }

    /// Read an array of [Unstashable] objects into a vector
    fn read_array_of_object_vec<C: Copy, T: 'static + Unstashable<C>>(
        &mut self,
        context: C,
    ) -> Result<Vec<T>, UnstashError> {
        self.read_array_of_object_iter(context)?.collect()
    }

    /// Read an array of [Unstashable] objects via an iterator
    fn read_array_of_object_iter<C: Copy, T: 'static + Unstashable<C>>(
        &mut self,
        context: C,
    ) -> Result<ObjectIterator<'a, C, T>, UnstashError> {
        self.reset_on_error(
            |unstasher, context| {
                if unstasher.read_value_type()? != ValueType::ArrayOfObjects {
                    return Err(UnstashError::WrongValueType);
                }
                let len = unstasher.read_value_length()?;

                let Some((hashes, remaining_hashes)) = unstasher.dependencies.split_at_checked(len)
                else {
                    return Err(UnstashError::Corrupted);
                };
                unstasher.dependencies = remaining_hashes;
                let iter = ObjectIterator {
                    hashes,
                    stashmap: unstasher.stashmap,
                    context: context,
                    _phantom_data: PhantomData,
                };
                Ok(iter)
            },
            context,
        )
    }

    /// Read an array of stashed objects via the given function which
    /// is called once per object with an [Unstasher] instance.
    fn read_array_of_object_proxies<
        Context: Copy,
        F: FnMut(&mut Unstasher<Context>) -> Result<(), UnstashError>,
    >(
        &mut self,
        mut f: F,
        context: Context,
    ) -> Result<(), UnstashError> {
        self.reset_on_error(
            |unstasher, context| {
                if unstasher.read_value_type()? != ValueType::ArrayOfObjects {
                    return Err(UnstashError::WrongValueType);
                }
                let len = unstasher.read_value_length()?;

                let Some((hashes, remaining_hashes)) = unstasher.dependencies.split_at_checked(len)
                else {
                    return Err(UnstashError::Corrupted);
                };
                unstasher.dependencies = remaining_hashes;
                for hash in hashes {
                    unstasher.stashmap.unstash(*hash, &mut f, context)?;
                }
                Ok(())
            },
            context,
        )
    }

    /// Read a single given [UnstashableInplace] object with the given phase
    fn object_inplace<C: Copy, T: UnstashableInplace<C>>(
        &mut self,
        object: &mut T,
        phase: InplaceUnstashPhase,
        context: C,
    ) -> Result<(), UnstashError> {
        self.reset_on_error(
            |unstasher, context| {
                if unstasher.read_value_type()? != ValueType::StashedObject {
                    return Err(UnstashError::WrongValueType);
                }

                let hash = unstasher.read_dependency()?;
                unstasher.stashmap.unstash_inplace(
                    hash,
                    phase,
                    |unstasher| object.unstash_inplace(unstasher),
                    context,
                )
            },
            context,
        )
    }

    /// Read a single object via a given function that receives an [Unstasher]
    fn object_proxy<Context: Copy, R: 'static, F>(
        &mut self,
        f: F,
        context: Context,
    ) -> Result<R, UnstashError>
    where
        F: FnMut(&mut Unstasher<Context>) -> Result<R, UnstashError>,
    {
        self.reset_on_error(
            |unstasher, context| {
                if unstasher.read_value_type()? != ValueType::StashedObject {
                    return Err(UnstashError::WrongValueType);
                }

                let hash = unstasher.read_dependency()?;
                unstasher.stashmap.unstash(hash, f, context)
            },
            context,
        )
    }

    /// Read a single object via a given function that receives an [InplaceUnstasher]
    fn object_proxy_inplace<Context: Copy, F>(
        &mut self,
        f: F,
        phase: InplaceUnstashPhase,
        context: Context,
    ) -> Result<(), UnstashError>
    where
        F: FnMut(&mut InplaceUnstasher<Context>) -> Result<(), UnstashError>,
    {
        self.reset_on_error(
            |unstasher, context| {
                if unstasher.read_value_type()? != ValueType::StashedObject {
                    return Err(UnstashError::WrongValueType);
                }

                let hash = unstasher.read_dependency()?;
                unstasher.stashmap.unstash_inplace(hash, phase, f, context)
            },
            context,
        )
    }

    /// Read a single string
    fn string(&mut self) -> Result<String, UnstashError> {
        self.reset_on_error(
            |unstasher, _| {
                if unstasher.read_value_type()? != ValueType::String {
                    return Err(UnstashError::WrongValueType);
                }
                let len = unstasher.read_value_length()?;

                let slice = unstasher.read_raw_bytes(len)?;
                let str_slice = std::str::from_utf8(slice).map_err(|_| UnstashError::Corrupted)?;
                Ok(str_slice.to_string())
            },
            (),
        )
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

/// Struct for unstashing and deserializing by creating new objects.
/// This struct is passed to [Unstashable::unstash]
pub struct Unstasher<'a, Context = ()> {
    backend: UnstasherBackend<'a>,
    context: Context,
}

impl<'a, Context> Unstasher<'a, Context> {
    /// Create a new instance
    pub(crate) fn new(backend: UnstasherBackend<'a>, context: Context) -> Unstasher<'a, Context> {
        Unstasher { backend, context }
    }

    /// Get the backend
    pub(crate) fn backend(&self) -> &UnstasherBackend<'a> {
        &self.backend
    }
}

impl<'a, Context: Copy> Unstasher<'a, Context> {
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

    /// Read a single usize value
    pub fn usize(&mut self) -> Result<usize, UnstashError> {
        self.backend.read_primitive::<u64>().map(|x| x as usize)
    }

    /// Read a single isize value
    pub fn isize(&mut self) -> Result<isize, UnstashError> {
        self.backend.read_primitive::<i64>().map(|x| x as isize)
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
    pub fn array_of_i8_iter(&mut self) -> Result<PrimitiveIterator<'a, i8>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of u8 values via an iterator
    pub fn array_of_u8_iter(&mut self) -> Result<PrimitiveIterator<'a, u8>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of i16 values via an iterator
    pub fn array_of_i16_iter(&mut self) -> Result<PrimitiveIterator<'a, i16>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of u16 values via an iterator
    pub fn array_of_u16_iter(&mut self) -> Result<PrimitiveIterator<'a, u16>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of i32 values via an iterator
    pub fn array_of_i32_iter(&mut self) -> Result<PrimitiveIterator<'a, i32>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of u32 values via an iterator
    pub fn array_of_u32_iter(&mut self) -> Result<PrimitiveIterator<'a, u32>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of i64 values via an iterator
    pub fn array_of_i64_iter(&mut self) -> Result<PrimitiveIterator<'a, i64>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of u64 values via an iterator
    pub fn array_of_u64_iter(&mut self) -> Result<PrimitiveIterator<'a, u64>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of f32 values via an iterator
    pub fn array_of_f32_iter(&mut self) -> Result<PrimitiveIterator<'a, f32>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of f64 values via an iterator
    pub fn array_of_f64_iter(&mut self) -> Result<PrimitiveIterator<'a, f64>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of [Unstashable] objects into a vector
    pub fn array_of_objects_vec<T: 'static + Unstashable<Context>>(
        &mut self,
    ) -> Result<Vec<T>, UnstashError> {
        self.array_of_objects_vec_with_context(self.context)
    }

    pub fn array_of_objects_vec_with_context<C1: Copy, T: 'static + Unstashable<C1>>(
        &mut self,
        context: C1,
    ) -> Result<Vec<T>, UnstashError> {
        self.backend.read_array_of_object_vec(context)
    }

    /// Read an array of [Unstashable] objects into an iterator
    pub fn array_of_objects_iter<T: 'static + Unstashable<Context>>(
        &mut self,
    ) -> Result<ObjectIterator<'_, Context, T>, UnstashError> {
        self.array_of_objects_iter_with_context(self.context)
    }

    pub fn array_of_objects_iter_with_context<C1: Copy, T: 'static + Unstashable<C1>>(
        &mut self,
        context: C1,
    ) -> Result<ObjectIterator<'_, C1, T>, UnstashError> {
        self.backend.read_array_of_object_iter(context)
    }

    /// Read an array of objects via a function receiving an [Unstasher] for each object
    pub fn array_of_proxy_objects<F>(&mut self, f: F) -> Result<(), UnstashError>
    where
        F: FnMut(&mut Unstasher<Context>) -> Result<(), UnstashError>,
    {
        self.array_of_proxy_objects_with_context(f, self.context)
    }

    pub fn array_of_proxy_objects_with_context<OtherContext: Copy, F>(
        &mut self,
        f: F,
        context: OtherContext,
    ) -> Result<(), UnstashError>
    where
        F: FnMut(&mut Unstasher<OtherContext>) -> Result<(), UnstashError>,
    {
        self.backend.read_array_of_object_proxies(f, context)
    }

    /// Read a single string
    pub fn string(&mut self) -> Result<String, UnstashError> {
        self.backend.string()
    }

    /// Read a single [Unstashable] object
    pub fn object<T: 'static + Unstashable<Context>>(&mut self) -> Result<T, UnstashError> {
        self.object_with_context(self.context)
    }

    pub fn object_with_context<C1: Copy, T: 'static + Unstashable<C1>>(
        &mut self,
        context: C1,
    ) -> Result<T, UnstashError> {
        self.backend.object_proxy(T::unstash, context)
    }

    /// Read a single [UnstashableInplace] object
    pub fn object_inplace<T: UnstashableInplace<Context>>(
        &mut self,
        object: &mut T,
    ) -> Result<(), UnstashError> {
        self.object_inplace_with_context(object, self.context)
    }

    pub fn object_inplace_with_context<C1: Copy, T: UnstashableInplace<C1>>(
        &mut self,
        object: &mut T,
        context: C1,
    ) -> Result<(), UnstashError> {
        let backend_original = self.backend.clone();
        self.backend
            .object_inplace(object, InplaceUnstashPhase::Validate, context)?;
        self.backend = backend_original;
        self.backend
            .object_inplace(object, InplaceUnstashPhase::Write, context)
    }

    /// Read a single object via a function receiving an [Unstasher]
    pub fn object_proxy<T: 'static, F>(&mut self, f: F) -> Result<T, UnstashError>
    where
        F: FnMut(&mut Unstasher<Context>) -> Result<T, UnstashError>,
    {
        self.object_proxy_with_context(f, self.context)
    }

    pub fn object_proxy_with_context<OtherContext: Copy, T: 'static, F>(
        &mut self,
        f: F,
        context: OtherContext,
    ) -> Result<T, UnstashError>
    where
        F: FnMut(&mut Unstasher<OtherContext>) -> Result<T, UnstashError>,
    {
        self.backend.object_proxy(f, context)
    }

    /// Read a single object via a function receiving an [InplaceUnstasher].
    /// The function is called twice, once to validate and once to write.
    /// No lasting changes should be made unless [InplaceUnstasher::time_to_write]
    /// returns true. Data should always be read to catch unstashing errors
    /// during the validation phase, before persistent changes are made.
    pub fn object_proxy_inplace<F>(&mut self, f: F) -> Result<(), UnstashError>
    where
        F: FnMut(&mut InplaceUnstasher<Context>) -> Result<(), UnstashError>,
    {
        self.object_proxy_inplace_with_context(f, self.context)
    }

    pub fn object_proxy_inplace_with_context<OtherContext: Copy, F>(
        &mut self,
        mut f: F,
        context: OtherContext,
    ) -> Result<(), UnstashError>
    where
        F: FnMut(&mut InplaceUnstasher<OtherContext>) -> Result<(), UnstashError>,
    {
        let backend_original = self.backend.clone();
        self.backend
            .object_proxy_inplace(&mut f, InplaceUnstashPhase::Validate, context)?;
        self.backend = backend_original;
        self.backend
            .object_proxy_inplace(&mut f, InplaceUnstashPhase::Write, context)
    }

    /// Get the type of the next value, if one exists
    pub fn peek_type(&self) -> Result<ValueType, UnstashError> {
        self.backend.peek_type()
    }

    /// Get the length of the next value, if it has one.
    /// For arrays, this is the number of objects.
    /// For strings, this is the number of bytes in its UTF-8 encoding.
    pub fn peek_length(&self) -> Result<usize, UnstashError> {
        self.backend.peek_length()
    }

    /// Is there no data left to read?
    pub fn is_empty(&self) -> bool {
        self.backend.is_empty()
    }

    pub fn context(&self) -> Context {
        self.context
    }
}

/// The two phases of in-place unstashing, used to separate validation
/// and error detection from object modification for improved safety
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum InplaceUnstashPhase {
    /// The stashed contents are being validated and the object should
    /// not be written to. All the same contents should be unstashed.
    Validate,

    /// The stashed contents have been validated and should now be
    /// written to the object. All the same contents should be unstashed.
    Write,
}

/// Struct for unstashing and deserializing by modifying existing objects.
/// This struct is passed to [UnstashableInplace::unstash_inplace]
pub struct InplaceUnstasher<'a, Context = ()> {
    backend: UnstasherBackend<'a>,
    phase: InplaceUnstashPhase,
    context: Context,
}

impl<'a, Context> InplaceUnstasher<'a, Context> {
    /// Create a new unstasher with the given backend and phase
    pub(crate) fn new(
        backend: UnstasherBackend<'a>,
        phase: InplaceUnstashPhase,
        context: Context,
    ) -> InplaceUnstasher<'a, Context> {
        InplaceUnstasher {
            backend,
            phase,
            context,
        }
    }

    /// Get the backend
    pub(crate) fn backend(&self) -> &UnstasherBackend<'a> {
        &self.backend
    }

    /// Read a single primitive. The reference is only written
    /// to during the Write phase.
    fn read_primitive_inplace<T: 'static + PrimitiveReadWrite>(
        &mut self,
        x: &mut T,
    ) -> Result<(), UnstashError> {
        let y = self.backend.read_primitive::<T>()?;
        if self.phase == InplaceUnstashPhase::Write {
            *x = y;
        }
        Ok(())
    }

    /// Read an array of primitives to a vector. The reference is
    /// only written to during the Write phase.
    fn read_primitive_array_vec_inplace<T: 'static + PrimitiveReadWrite>(
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

impl<'a, Context: Copy> InplaceUnstasher<'a, Context> {
    /// Read a single bool value. The reference is only written
    /// to during the Write phase.
    pub fn bool_inplace(&mut self, x: &mut bool) -> Result<(), UnstashError> {
        self.read_primitive_inplace(x)
    }

    /// Read a single u8 value. The reference is only written
    /// to during the Write phase.
    pub fn u8_inplace(&mut self, x: &mut u8) -> Result<(), UnstashError> {
        self.read_primitive_inplace(x)
    }

    /// Read a single i8 value. The reference is only written
    /// to during the Write phase.
    pub fn i8_inplace(&mut self, x: &mut i8) -> Result<(), UnstashError> {
        self.read_primitive_inplace(x)
    }

    /// Read a single u16 value. The reference is only written
    /// to during the Write phase.
    pub fn u16_inplace(&mut self, x: &mut u16) -> Result<(), UnstashError> {
        self.read_primitive_inplace(x)
    }

    /// Read a single i16 value. The reference is only written
    /// to during the Write phase.
    pub fn i16_inplace(&mut self, x: &mut i16) -> Result<(), UnstashError> {
        self.read_primitive_inplace(x)
    }

    /// Read a single u32 value. The reference is only written
    /// to during the Write phase.
    pub fn u32_inplace(&mut self, x: &mut u32) -> Result<(), UnstashError> {
        self.read_primitive_inplace(x)
    }

    /// Read a single i32 value. The reference is only written
    /// to during the Write phase.
    pub fn i32_inplace(&mut self, x: &mut i32) -> Result<(), UnstashError> {
        self.read_primitive_inplace(x)
    }

    /// Read a single u64 value. The reference is only written
    /// to during the Write phase.
    pub fn u64_inplace(&mut self, x: &mut u64) -> Result<(), UnstashError> {
        self.read_primitive_inplace(x)
    }

    /// Read a single i64 value. The reference is only written
    /// to during the Write phase.
    pub fn i64_inplace(&mut self, x: &mut i64) -> Result<(), UnstashError> {
        self.read_primitive_inplace(x)
    }

    /// Read a single usize value. The reference is only written
    /// to during the Write phase.
    pub fn usize_inplace(&mut self, x: &mut usize) -> Result<(), UnstashError> {
        let v = self.u64_always()?;
        if self.phase == InplaceUnstashPhase::Write {
            *x = v as _;
        }
        Ok(())
    }

    /// Read a single isize value. The reference is only written
    /// to during the Write phase.
    pub fn isize_inplace(&mut self, x: &mut isize) -> Result<(), UnstashError> {
        let v = self.i64_always()?;
        if self.phase == InplaceUnstashPhase::Write {
            *x = v as _;
        }
        Ok(())
    }

    /// Read a single f32 value. The reference is only written
    /// to during the Write phase.
    pub fn f32_inplace(&mut self, x: &mut f32) -> Result<(), UnstashError> {
        self.read_primitive_inplace(x)
    }

    /// Read a single f64 value. The reference is only written
    /// to during the Write phase.
    pub fn f64_inplace(&mut self, x: &mut f64) -> Result<(), UnstashError> {
        self.read_primitive_inplace(x)
    }

    /// Read a single bool value directly.
    /// Lasting modifications to data structures should only be made
    /// when [Self::time_to_write] returns `true`
    pub fn bool_always(&mut self) -> Result<bool, UnstashError> {
        self.backend.read_primitive()
    }

    /// Read a single u8 value directly.
    /// Lasting modifications to data structures should only be made
    /// when [Self::time_to_write] returns `true`
    pub fn u8_always(&mut self) -> Result<u8, UnstashError> {
        self.backend.read_primitive()
    }

    /// Read a single i8 value directly.
    /// Lasting modifications to data structures should only be made
    /// when [Self::time_to_write] returns `true`
    pub fn i8_always(&mut self) -> Result<i8, UnstashError> {
        self.backend.read_primitive()
    }

    /// Read a single u16 value directly.
    /// Lasting modifications to data structures should only be made
    /// when [Self::time_to_write] returns `true`
    pub fn u16_always(&mut self) -> Result<u16, UnstashError> {
        self.backend.read_primitive()
    }

    /// Read a single i16 value directly.
    /// Lasting modifications to data structures should only be made
    /// when [Self::time_to_write] returns `true`
    pub fn i16_always(&mut self) -> Result<i16, UnstashError> {
        self.backend.read_primitive()
    }

    /// Read a single u32 value directly.
    /// Lasting modifications to data structures should only be made
    /// when [Self::time_to_write] returns `true`
    pub fn u32_always(&mut self) -> Result<u32, UnstashError> {
        self.backend.read_primitive()
    }

    /// Read a single i32 value directly.
    /// Lasting modifications to data structures should only be made
    /// when [Self::time_to_write] returns `true`
    pub fn i32_always(&mut self) -> Result<i32, UnstashError> {
        self.backend.read_primitive()
    }

    /// Read a single u64 value directly.
    /// Lasting modifications to data structures should only be made
    /// when [Self::time_to_write] returns `true`
    pub fn u64_always(&mut self) -> Result<u64, UnstashError> {
        self.backend.read_primitive()
    }

    /// Read a single i64 value directly.
    /// Lasting modifications to data structures should only be made
    /// when [Self::time_to_write] returns `true`
    pub fn i64_always(&mut self) -> Result<i64, UnstashError> {
        self.backend.read_primitive()
    }

    /// Read a single usize value directly.
    /// Lasting modifications to data structures should only be made
    /// when [Self::time_to_write] returns `true`
    pub fn usize_always(&mut self) -> Result<usize, UnstashError> {
        self.backend.read_primitive::<u64>().map(|x| x as usize)
    }

    /// Read a single isize value directly.
    /// Lasting modifications to data structures should only be made
    /// when [Self::time_to_write] returns `true`
    pub fn isize_always(&mut self) -> Result<isize, UnstashError> {
        self.backend.read_primitive::<i64>().map(|x| x as isize)
    }

    /// Read a single f32 value directly.
    /// Lasting modifications to data structures should only be made
    /// when [Self::time_to_write] returns `true`
    pub fn f32_always(&mut self) -> Result<f32, UnstashError> {
        self.backend.read_primitive()
    }

    /// Read a single f64 value directly.
    /// Lasting modifications to data structures should only be made
    /// when [Self::time_to_write] returns `true`
    pub fn f64_always(&mut self) -> Result<f64, UnstashError> {
        self.backend.read_primitive()
    }

    /// Read an array of u8 values into a Vec. The reference is only written
    /// to during the Write phase. Existing contents are completely overwritten.
    pub fn array_of_u8_vec_inplace(&mut self, x: &mut Vec<u8>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec_inplace(x)
    }

    /// Read an array of i8 values into a Vec. The reference is only written
    /// to during the Write phase. Existing contents are completely overwritten.
    pub fn array_of_i8_vec_inplace(&mut self, x: &mut Vec<i8>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec_inplace(x)
    }

    /// Read an array of u16 values into a Vec. The reference is only written
    /// to during the Write phase. Existing contents are completely overwritten.
    pub fn array_of_u16_vec_inplace(&mut self, x: &mut Vec<u16>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec_inplace(x)
    }

    /// Read an array of i16 values into a Vec. The reference is only written
    /// to during the Write phase. Existing contents are completely overwritten.
    pub fn array_of_i16_vec_inplace(&mut self, x: &mut Vec<i16>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec_inplace(x)
    }

    /// Read an array of u32 values into a Vec. The reference is only written
    /// to during the Write phase. Existing contents are completely overwritten.
    pub fn array_of_u32_vec_inplace(&mut self, x: &mut Vec<u32>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec_inplace(x)
    }

    /// Read an array of i32 values into a Vec. The reference is only written
    /// to during the Write phase. Existing contents are completely overwritten.
    pub fn array_of_i32_vec_inplace(&mut self, x: &mut Vec<i32>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec_inplace(x)
    }

    /// Read an array of u64 values into a Vec. The reference is only written
    /// to during the Write phase. Existing contents are completely overwritten.
    pub fn array_of_u64_vec_inplace(&mut self, x: &mut Vec<u64>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec_inplace(x)
    }

    /// Read an array of i64 values into a Vec. The reference is only written
    /// to during the Write phase. Existing contents are completely overwritten.
    pub fn array_of_i64_vec_inplace(&mut self, x: &mut Vec<i64>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec_inplace(x)
    }

    /// Read an array of f32 values into a Vec. The reference is only written
    /// to during the Write phase. Existing contents are completely overwritten.
    pub fn array_of_f32_vec_inplace(&mut self, x: &mut Vec<f32>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec_inplace(x)
    }

    /// Read an array of f64 values into a Vec. The reference is only written
    /// to during the Write phase. Existing contents are completely overwritten.
    pub fn array_of_f64_vec_inplace(&mut self, x: &mut Vec<f64>) -> Result<(), UnstashError> {
        self.read_primitive_array_vec_inplace(x)
    }

    /// Read an array of [Unstashable] objects into a Vec. The reference is
    /// only written to during the Write phase. Existing contents are completely
    /// overwritten.
    ///
    /// If you need to work with a different container or need more fine-grained
    /// control over how objects are written to, use [Self::array_of_proxy_objects]
    /// instead.
    pub fn array_of_objects_vec_inplace<T: 'static + Unstashable<Context>>(
        &mut self,
        x: &mut Vec<T>,
    ) -> Result<(), UnstashError> {
        self.array_of_objects_vec_inplace_with_context(x, self.context)
    }

    pub fn array_of_objects_vec_inplace_with_context<C1: Copy, T: 'static + Unstashable<C1>>(
        &mut self,
        x: &mut Vec<T>,
        context: C1,
    ) -> Result<(), UnstashError> {
        let v = self.backend.read_array_of_object_vec(context)?;
        if self.phase == InplaceUnstashPhase::Write {
            *x = v;
        }
        Ok(())
    }

    /// Read an array of u8 values via an iterator.
    /// Lasting modifications to data structures should only be made
    /// when [Self::time_to_write] returns `true`
    pub fn array_of_u8_iter(&mut self) -> Result<PrimitiveIterator<'a, u8>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of i8 values via an iterator.
    /// Lasting modifications to data structures should only be made
    /// when [Self::time_to_write] returns `true`
    pub fn array_of_i8_iter(&mut self) -> Result<PrimitiveIterator<'a, i8>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of u16 values via an iterator.
    /// Lasting modifications to data structures should only be made
    /// when [Self::time_to_write] returns `true`
    pub fn array_of_u16_iter(&mut self) -> Result<PrimitiveIterator<'a, u16>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of i16 values via an iterator.
    /// Lasting modifications to data structures should only be made
    /// when [Self::time_to_write] returns `true`
    pub fn array_of_i16_iter(&mut self) -> Result<PrimitiveIterator<'a, i16>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of u32 values via an iterator.
    /// Lasting modifications to data structures should only be made
    /// when [Self::time_to_write] returns `true`
    pub fn array_of_u32_iter(&mut self) -> Result<PrimitiveIterator<'a, u32>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of i32 values via an iterator.
    /// Lasting modifications to data structures should only be made
    /// when [Self::time_to_write] returns `true`
    pub fn array_of_i32_iter(&mut self) -> Result<PrimitiveIterator<'a, i32>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of u64 values via an iterator.
    /// Lasting modifications to data structures should only be made
    /// when [Self::time_to_write] returns `true`
    pub fn array_of_u64_iter(&mut self) -> Result<PrimitiveIterator<'a, u64>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of i64 values via an iterator.
    /// Lasting modifications to data structures should only be made
    /// when [Self::time_to_write] returns `true`
    pub fn array_of_i64_iter(&mut self) -> Result<PrimitiveIterator<'a, i64>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of f32 values via an iterator.
    /// Lasting modifications to data structures should only be made
    /// when [Self::time_to_write] returns `true`
    pub fn array_of_f32_iter(&mut self) -> Result<PrimitiveIterator<'a, f32>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of f64 values via an iterator.
    /// Lasting modifications to data structures should only be made
    /// when [Self::time_to_write] returns `true`
    pub fn array_of_f64_iter(&mut self) -> Result<PrimitiveIterator<'a, f64>, UnstashError> {
        self.backend.read_primitive_array_iter()
    }

    /// Read an array of objects and visit each with the given function that receives
    /// an [Unstasher] instance. This can be used to interface with more general kinds
    /// of containers and data structures at the cost of needing to know more about
    /// the underlying Validation and Write phases.
    ///
    /// To use this method correctly, data should always be read and unstashed,
    /// but actual modifications to data structures should only be done during the
    /// `Write` phase when [Self::time_to_write] returns true. Failure to do so may
    /// result in objects being left in unexpected states or duplicated modifications.
    ///
    /// See [crate::test_stash_roundtrip_inplace] for a way to automatically test
    /// whether this method is being used correctly.
    pub fn array_of_proxy_objects<F>(&mut self, f: F) -> Result<(), UnstashError>
    where
        F: FnMut(&mut Unstasher<Context>) -> Result<(), UnstashError>,
    {
        self.array_of_proxy_objects_with_context(f, self.context)
    }

    pub fn array_of_proxy_objects_with_context<OtherContext: Copy, F>(
        &mut self,
        f: F,
        context: OtherContext,
    ) -> Result<(), UnstashError>
    where
        F: FnMut(&mut Unstasher<OtherContext>) -> Result<(), UnstashError>,
    {
        self.backend.read_array_of_object_proxies(f, context)
    }

    /// Read a string. The reference is only written to during the Write phase.
    /// Existing contents are completely overwritten.
    pub fn string_inplace(&mut self, x: &mut String) -> Result<(), UnstashError> {
        let s = self.backend.string()?;
        if self.phase == InplaceUnstashPhase::Write {
            *x = s;
        }
        Ok(())
    }

    /// Read a string and return it directly, during both the validation
    /// and write phases. Lasting changes should only be made when
    /// [Self::time_to_write] is true.
    pub fn string_always(&mut self) -> Result<String, UnstashError> {
        self.backend.string()
    }

    /// Read an object which is [Unstashable]. The reference is only written to
    /// during the Write phase. The existing object is completely overwritten
    /// with the newly-unstashed object.
    pub fn object_replace<T: 'static + Unstashable<Context>>(
        &mut self,
        object: &mut T,
    ) -> Result<(), UnstashError> {
        self.object_replace_with_context(object, self.context)
    }

    pub fn object_replace_with_context<C1: Copy, T: 'static + Unstashable<C1>>(
        &mut self,
        object: &mut T,
        context: C1,
    ) -> Result<(), UnstashError> {
        let other_object = self.backend.object_proxy(T::unstash, context)?;
        if self.phase == InplaceUnstashPhase::Write {
            *object = other_object;
        }
        Ok(())
    }

    /// Read an object which is [Unstashable] and return it directly, during
    /// both the validation and write phases. Lasting changes should only be
    /// made when /// [Self::time_to_write] is true.
    pub fn object_always<T: 'static + Unstashable<Context>>(&mut self) -> Result<T, UnstashError> {
        self.object_always_with_context(self.context)
    }

    pub fn object_always_with_context<C1: Copy, T: 'static + Unstashable<C1>>(
        &mut self,
        context: C1,
    ) -> Result<T, UnstashError> {
        self.backend.object_proxy(T::unstash, context)
    }

    /// Read an object which is [UnstashableInplace]. The given reference is
    /// itself unstashed in place using the same phase as the current object.
    pub fn object_inplace<T: UnstashableInplace<Context>>(
        &mut self,
        object: &mut T,
    ) -> Result<(), UnstashError> {
        self.object_inplace_with_context(object, self.context)
    }

    pub fn object_inplace_with_context<C1: Copy, T: UnstashableInplace<C1>>(
        &mut self,
        object: &mut T,
        context: C1,
    ) -> Result<(), UnstashError> {
        self.backend.object_inplace(object, self.phase, context)
    }

    /// Read an object and with the given function that receives an [Unstasher]
    /// instance. This can be used to interface with more general kinds of
    /// containers and data structures at the cost of needing to know more about
    /// the underlying Validation and Write phases.
    ///
    /// To use this method correctly, objects should always be read and unstashed,
    /// but actual modifications to data structures should only be done during the
    /// `Write` phase when [Self::time_to_write] returns `true`. Failure to do so may
    /// result in objects being left in unexpected states or duplicated modifications.
    ///
    /// See [crate::test_stash_roundtrip_inplace] for a way to automatically test
    /// whether this method is being used correctly.
    pub fn object_proxy<R: 'static, F>(&mut self, f: F) -> Result<R, UnstashError>
    where
        F: FnMut(&mut Unstasher<Context>) -> Result<R, UnstashError>,
    {
        self.object_proxy_with_context(f, self.context)
    }

    pub fn object_proxy_with_context<OtherContext: Copy, R: 'static, F>(
        &mut self,
        f: F,
        context: OtherContext,
    ) -> Result<R, UnstashError>
    where
        F: FnMut(&mut Unstasher<OtherContext>) -> Result<R, UnstashError>,
    {
        self.backend.object_proxy(f, context)
    }

    pub fn object_proxy_inplace<F>(&mut self, f: F) -> Result<(), UnstashError>
    where
        F: FnMut(&mut InplaceUnstasher<Context>) -> Result<(), UnstashError>,
    {
        self.object_proxy_inplace_with_context(f, self.context)
    }

    pub fn object_proxy_inplace_with_context<OtherContext: Copy, F>(
        &mut self,
        f: F,
        context: OtherContext,
    ) -> Result<(), UnstashError>
    where
        F: FnMut(&mut InplaceUnstasher<OtherContext>) -> Result<(), UnstashError>,
    {
        self.backend.object_proxy_inplace(f, self.phase, context)
    }

    /// Are we in the write phase, i.e. should lasting changes be made to the
    /// data structures we're unstashing? If not, the same data should be read
    /// but as a practice run, without mutating the underlying data structures.
    pub fn time_to_write(&self) -> bool {
        self.phase == InplaceUnstashPhase::Write
    }

    /// Get the type of the next stashed object, if there is one
    pub fn peek_type(&self) -> Result<ValueType, UnstashError> {
        self.backend.peek_type()
    }

    /// Get the length of the next value, if it has one.
    /// For arrays, this is the number of objects.
    /// For strings, this is the number of bytes in its UTF-8 encoding.
    pub fn peek_length(&self) -> Result<usize, UnstashError> {
        self.backend.peek_length()
    }

    /// Is there no data left?
    pub fn is_empty(&self) -> bool {
        self.backend.is_empty()
    }

    pub fn context(&self) -> Context {
        self.context
    }
}
