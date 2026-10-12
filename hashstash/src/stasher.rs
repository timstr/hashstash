use std::hash::Hasher;

use crate::{valuetypes::PrimitiveReadWrite, ObjectHash, StashMap, Stashable, ValueType};

/// A [Stasher] backend which hashes the object contents
struct HashingStasher<'a> {
    hasher: &'a mut seahash::SeaHasher,
    current_unordered_hash: Option<u64>,
}

/// A [Stasher] backend which serializes the object contents
struct SerializingStasher<'a> {
    /// Vector of bytes into which new raw data will be written
    data: &'a mut Vec<u8>,

    /// List of explicit dependencies to other stashed objects
    /// that object hashes will be added to
    dependencies: &'a mut Vec<ObjectHash>,

    /// The stashmap into which we are serializing
    stashmap: &'a mut StashMap,
}

/// The backend implementation of a [Stasher]
enum StasherBackend<'a> {
    /// Stashing and hashing, reading object contents only to
    /// compute an ObjectHash summary.
    Hash(HashingStasher<'a>),

    /// Stashing and serializing, reading object contents and
    /// persisting them into a Stash.
    Serialize(SerializingStasher<'a>),
}

/// Whether order matters for an array of stashed objects
pub enum Order {
    /// Order matters. Permuting the objects results in
    /// a different ObjectHash.
    Ordered,

    /// Order does not matter. Permuting the objects results
    /// in an equivalent ObjectHash.
    Unordered,
}

/// Used when serializing sequences to know where to write
/// the length prefix after the count is known.
struct SequenceBookmark(usize);

impl<'a> StasherBackend<'a> {
    /// Write a slice of raw bytes
    fn write_raw_bytes(&mut self, bytes: &[u8]) {
        match self {
            StasherBackend::Hash(hash) => {
                hash.hasher.write(bytes);
            }
            StasherBackend::Serialize(serialize) => serialize.data.extend_from_slice(bytes),
        }
    }

    /// Stash and track a dependency. When hashing, this simply
    /// hashes the object. When serializing, this stashes the
    /// object in the stashmap and adds a reference to it.
    fn stash_dependency<OtherContext: Copy, F: FnMut(&mut Stasher<'_, OtherContext>)>(
        &mut self,
        f: F,
        context: OtherContext,
    ) {
        match self {
            StasherBackend::Hash(hasher) => {
                let hash = ObjectHash::with_stasher_and_context(f, context);
                match hasher.current_unordered_hash.as_mut() {
                    Some(unorderd_hash) => *unorderd_hash ^= hash.0,
                    None => hasher.hasher.write_u64(hash.0),
                }
            }
            StasherBackend::Serialize(serializer) => {
                // TODO: consider adding a small object optimization.
                // For example if the serialized object contents take
                // up no more space than a reference to the contents
                // elsewhere in the stashmap, then just store the contents
                // directly rather than adding a dependency. This
                // could be done cleverly by first extending the
                // hashing backend to sum the content size, allowing
                // this decision to be made after the ObjectHash has
                // been computed but before the stashmap is modified.
                let hash = serializer.stashmap.stash_and_add_reference(f, context);
                serializer.dependencies.push(hash);
            }
        }
    }

    /// Start a sequence of objects. When hashing, this
    /// instructs the hasher whether to combine hashes of
    /// subsequent objects in an order-sensitive or order-
    /// insensitive manner. When serializing, this makes
    /// space to store a prefixed length.
    fn begin_sequence(&mut self, ordering: Order) -> SequenceBookmark {
        match self {
            StasherBackend::Hash(hasher) => {
                if let Order::Unordered = ordering {
                    hasher.current_unordered_hash = Some(0);
                }

                // This will not be used
                SequenceBookmark(usize::MAX)
            }
            StasherBackend::Serialize(serializer) => {
                let bookmark = serializer.data.len();
                let placeholder_length: u32 = 0;
                for b in placeholder_length.to_be_bytes() {
                    serializer.data.push(b);
                }

                // Where to write the length prefix later
                SequenceBookmark(bookmark)
            }
        }
    }

    /// Complete a sequence of objects. When hashing, this
    /// simply hashes the length. When serializing, this
    /// writes the length at the previously bookmarked location.
    fn end_sequence(&mut self, bookmark: SequenceBookmark, length: u32) {
        match self {
            StasherBackend::Hash(hasher) => {
                if let Some(hash) = hasher.current_unordered_hash.take() {
                    hasher.hasher.write_u64(hash);
                }
                hasher.hasher.write_u32(length)
            }
            StasherBackend::Serialize(serializer) => {
                for (i, b) in length.to_be_bytes().into_iter().enumerate() {
                    serializer.data[bookmark.0 + i] = b;
                }
            }
        }
    }
}

/// A stasher is used to visit the contents of an object as part of its
/// [Stashable::stash] implementation, interchangeably to both hash and
/// to serialize those contents.
pub struct Stasher<'a, Context = ()> {
    backend: StasherBackend<'a>,
    context: Context,
}

/// Private methods
impl<'a, Context> Stasher<'a, Context> {
    /// Create a new Stasher for serializing
    pub(crate) fn new_serializer(
        data: &'a mut Vec<u8>,
        dependencies: &'a mut Vec<ObjectHash>,
        stashmap: &'a mut StashMap,
        context: Context,
    ) -> Stasher<'a, Context> {
        Stasher {
            backend: StasherBackend::Serialize(SerializingStasher {
                data,
                dependencies,
                stashmap,
            }),
            context,
        }
    }

    /// Create a new Stasher for hashing
    pub(crate) fn new_hasher(
        hasher: &'a mut seahash::SeaHasher,
        context: Context,
    ) -> Stasher<'a, Context> {
        Stasher {
            backend: StasherBackend::Hash(HashingStasher {
                hasher,
                current_unordered_hash: None,
            }),
            context,
        }
    }

    /// Write a sequence of raw bytes
    pub(crate) fn write_raw_bytes(&mut self, bytes: &[u8]) {
        self.backend.write_raw_bytes(bytes);
    }

    /// Helper method to write a single primitive
    fn write_primitive<T: PrimitiveReadWrite>(&mut self, x: T) {
        self.write_raw_bytes(&[ValueType::Primitive(T::TYPE).to_byte()]);
        x.write_raw_bytes_to(self);
    }

    /// Helper method to write a slice of primitives
    fn write_primitive_array<T: PrimitiveReadWrite, I: Iterator<Item = T>>(&mut self, it: I) {
        self.backend
            .write_raw_bytes(&[ValueType::Array(T::TYPE).to_byte()]);
        let bookmark = self.backend.begin_sequence(Order::Ordered);
        let mut length: u32 = 0;
        for x in it {
            x.write_raw_bytes_to(self);
            length += 1;
        }
        self.backend.end_sequence(bookmark, length);
    }

    /// Returns true iff the backend is hashing and not serializing
    pub(crate) fn hashing(&self) -> bool {
        match &self.backend {
            StasherBackend::Hash(_) => true,
            StasherBackend::Serialize(_) => false,
        }
    }
}

/// Public methods
impl<'a, Context: Copy> Stasher<'a, Context> {
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

    /// Write a single usize value. Internally, this is always u64.
    pub fn usize(&mut self, x: usize) {
        self.write_primitive(x as u64);
    }

    /// Write a single isize value. Internally, this is always i64.
    pub fn isize(&mut self, x: isize) {
        self.write_primitive(x as i64);
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

    /// Write a single [Stashable] object
    pub fn object<T: Stashable<Context>>(&mut self, object: &T) {
        self.object_with_context(object, self.context);
    }

    pub fn object_with_context<C1: Copy, T: Stashable<C1>>(&mut self, object: &T, context: C1) {
        self.write_raw_bytes(&[ValueType::StashedObject.to_byte()]);
        self.backend
            .stash_dependency(|stasher| object.stash(stasher), context);
    }

    /// Write a single object via a function receiving a [Stasher].
    pub fn object_proxy<F>(&mut self, f: F)
    where
        F: FnMut(&mut Stasher<'_, Context>),
    {
        self.object_proxy_with_context(f, self.context);
    }

    pub fn object_proxy_with_context<OtherContext: Copy, F>(&mut self, f: F, context: OtherContext)
    where
        F: FnMut(&mut Stasher<'_, OtherContext>),
    {
        self.write_raw_bytes(&[ValueType::StashedObject.to_byte()]);
        self.backend.stash_dependency(f, context);
    }

    /// Write an array of [Stashable] objects from a slice
    pub fn array_of_objects_slice<T: Stashable<Context>>(&mut self, objects: &[T], order: Order) {
        self.array_of_objects_iter_with_context(objects.iter(), order, self.context);
    }

    pub fn array_of_objects_slice_with_context<C1: Copy, T: Stashable<C1>>(
        &mut self,
        objects: &[T],
        order: Order,
        context: C1,
    ) {
        self.array_of_objects_iter_with_context(objects.iter(), order, context);
    }

    /// Write an array of [Stashable] objects from an iterator
    pub fn array_of_objects_iter<'b, T: 'b + Stashable<Context>, I: Iterator<Item = &'b T>>(
        &mut self,
        it: I,
        order: Order,
    ) {
        self.array_of_objects_iter_with_context(it, order, self.context);
    }

    pub fn array_of_objects_iter_with_context<
        'b,
        C1: Copy,
        T: 'b + Stashable<C1>,
        I: Iterator<Item = &'b T>,
    >(
        &mut self,
        it: I,
        order: Order,
        context: C1,
    ) {
        self.backend
            .write_raw_bytes(&[ValueType::ArrayOfObjects.to_byte()]);
        let bookmark = self.backend.begin_sequence(order);
        let mut length: u32 = 0;
        for object in it {
            self.backend
                .stash_dependency(|stasher| object.stash(stasher), context);
            length += 1;
        }
        self.backend.end_sequence(bookmark, length);
    }

    /// Write an array of objects from an intermediate iterator and function
    /// which stashes each item's contents.
    pub fn array_of_proxy_objects<T, I: Iterator<Item = T>, F>(&mut self, it: I, f: F, order: Order)
    where
        F: FnMut(&T, &mut Stasher<'_, Context>),
    {
        self.array_of_proxy_objects_with_context(it, f, order, self.context);
    }

    pub fn array_of_proxy_objects_with_context<OtherContext: Copy, T, I: Iterator<Item = T>, F>(
        &mut self,
        it: I,
        mut f: F,
        order: Order,
        context: OtherContext,
    ) where
        F: FnMut(&T, &mut Stasher<'_, OtherContext>),
    {
        self.backend
            .write_raw_bytes(&[ValueType::ArrayOfObjects.to_byte()]);
        let bookmark = self.backend.begin_sequence(order);
        let mut length: u32 = 0;
        for object in it {
            self.backend.stash_dependency(
                |stasher: &mut Stasher<'_, OtherContext>| f(&object, stasher),
                context,
            );
            length += 1;
        }
        self.backend.end_sequence(bookmark, length);
    }

    /// Write a single string
    pub fn string(&mut self, x: &str) {
        self.backend.write_raw_bytes(&[ValueType::String.to_byte()]);
        let bookmark = self.backend.begin_sequence(Order::Ordered);
        let bytes = x.as_bytes();
        self.write_raw_bytes(bytes);
        self.backend.end_sequence(bookmark, bytes.len() as u32);
    }

    pub fn context(&self) -> Context {
        self.context
    }
}
