use crate::{stasher::Stasher, UnstashError};

/// Enum for the set of primitive fixed-size types that are supported
#[derive(PartialEq, Eq, Debug)]
pub enum PrimitiveType {
    Bool,
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64,
    F32,
    F64,
}

/// Enum for set the of value types that are supported
#[derive(PartialEq, Eq, Debug)]
pub enum ValueType {
    /// A fixed-size primitive, e.g. boolean, integer, or floating point number
    Primitive(PrimitiveType),

    /// A list of values of a common primitive type whose number of elements can be queried
    Array(PrimitiveType),

    /// A utf-8 encoded string
    String,

    /// Another object elsewhere in the stash
    StashedObject,
}

impl PrimitiveType {
    /// Returns an integer with value 0xF or less, used to uniquely tag each primitive type
    fn to_nibble(&self) -> u8 {
        match self {
            PrimitiveType::Bool => 0x01,
            PrimitiveType::U8 => 0x02,
            PrimitiveType::I8 => 0x03,
            PrimitiveType::U16 => 0x04,
            PrimitiveType::I16 => 0x05,
            PrimitiveType::U32 => 0x06,
            PrimitiveType::I32 => 0x07,
            PrimitiveType::U64 => 0x08,
            PrimitiveType::I64 => 0x09,
            PrimitiveType::F32 => 0x0A,
            PrimitiveType::F64 => 0x0B,
        }
    }

    /// Constructs a PrimitiveType from an integer value as returned by to_nibble()
    fn from_nibble(byte: u8) -> Result<PrimitiveType, UnstashError> {
        match byte {
            0x01 => Ok(PrimitiveType::Bool),
            0x02 => Ok(PrimitiveType::U8),
            0x03 => Ok(PrimitiveType::I8),
            0x04 => Ok(PrimitiveType::U16),
            0x05 => Ok(PrimitiveType::I16),
            0x06 => Ok(PrimitiveType::U32),
            0x07 => Ok(PrimitiveType::I32),
            0x08 => Ok(PrimitiveType::U64),
            0x09 => Ok(PrimitiveType::I64),
            0x0A => Ok(PrimitiveType::F32),
            0x0B => Ok(PrimitiveType::F64),
            _ => Err(UnstashError::Corrupted),
        }
    }
}

impl ValueType {
    /// Returns an integer used to uniquely tag each value type
    pub(crate) fn to_byte(&self) -> u8 {
        match self {
            ValueType::Primitive(prim_type) => 0x00 | prim_type.to_nibble(),
            ValueType::Array(prim_type) => 0x10 | prim_type.to_nibble(),
            ValueType::String => 0x20,
            ValueType::StashedObject => 0x30,
        }
    }

    /// Constructs a ValueType from an integer value as returned by to_byte()
    pub(crate) fn from_byte(byte: u8) -> Result<ValueType, UnstashError> {
        let hi_nibble = byte & 0xF0;
        let lo_nibble = byte & 0x0F;
        match hi_nibble {
            0x00 => Ok(ValueType::Primitive(PrimitiveType::from_nibble(lo_nibble)?)),
            0x10 => Ok(ValueType::Array(PrimitiveType::from_nibble(lo_nibble)?)),
            0x20 => Ok(ValueType::String),
            0x30 => Ok(ValueType::StashedObject),
            _ => Err(UnstashError::Corrupted),
        }
    }
}

/// Helper trait for serializing primitives directly
pub(crate) trait PrimitiveReadWrite {
    /// The number of bytes occupied by the value itself in memory
    const SIZE: usize;

    /// The PrimitiveType that this type corresponds to, e.g. PrimitiveType::I32 for i32
    const TYPE: PrimitiveType;

    /// Write self to the byte vector
    fn write_raw_bytes_to(&self, stasher: &mut Stasher);

    /// Read self from the byte slice, moving it forward.
    /// This method may panic if there are fewer than Self::SIZE bytes remaining
    fn read_raw_bytes_from(bytes: &mut &[u8]) -> Self;
}

/// Macro for implementing the PrimitiveReadWrite helper trait for a given
/// Rust type, given its size in bytes and its corresponding PrimitiveType.
/// The methods `to_be_bytes()` and `from_be_bytes` are used, which exist
/// for all primitive integer and floating point types
macro_rules! impl_primitive_read_write {
    ($primitive: ident, $size: literal, $typetag: expr) => {
        impl PrimitiveReadWrite for $primitive {
            const SIZE: usize = $size;
            const TYPE: PrimitiveType = $typetag;
            fn write_raw_bytes_to(&self, stasher: &mut Stasher) {
                stasher.write_raw_bytes(&self.to_be_bytes());
            }
            fn read_raw_bytes_from(bytes: &mut &[u8]) -> Self {
                let (head, rest) = bytes.split_first_chunk::<$size>().unwrap();
                *bytes = rest;
                Self::from_be_bytes(*head)
            }
        }
    };
}

impl_primitive_read_write!(u8, 1, PrimitiveType::U8);
impl_primitive_read_write!(i8, 1, PrimitiveType::I8);
impl_primitive_read_write!(u16, 2, PrimitiveType::U16);
impl_primitive_read_write!(i16, 2, PrimitiveType::I16);
impl_primitive_read_write!(u32, 4, PrimitiveType::U32);
impl_primitive_read_write!(i32, 4, PrimitiveType::I32);
impl_primitive_read_write!(u64, 8, PrimitiveType::U64);
impl_primitive_read_write!(i64, 8, PrimitiveType::I64);
impl_primitive_read_write!(f32, 4, PrimitiveType::F32);
impl_primitive_read_write!(f64, 8, PrimitiveType::F64);

/// Explicit implementation of PrimitiveReadWrite for bool,
/// which does not have from_be_bytes() / to_be_bytes()
impl PrimitiveReadWrite for bool {
    const SIZE: usize = 1;
    const TYPE: PrimitiveType = PrimitiveType::Bool;

    fn write_raw_bytes_to(&self, stasher: &mut Stasher) {
        stasher.write_raw_bytes(&[if *self { 1 } else { 0 }]);
    }

    fn read_raw_bytes_from(bytes: &mut &[u8]) -> bool {
        let (byte, rest) = bytes.split_first().unwrap();
        *bytes = rest;
        *byte == 1
    }
}
