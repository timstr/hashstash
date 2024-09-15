use rand::prelude::*;

use std::collections::{HashMap, HashSet};

use crate::{
    test_stash_roundtrip, test_stash_roundtrip_inplace, InplaceUnstashPhase, InplaceUnstasher,
    Order, Stash, Stashable, Stasher, UnstashError, Unstashable, UnstashableInplace, Unstasher,
};

#[derive(Clone, Eq, PartialEq, Debug, Hash)]
struct StructA {
    i: i32,
    x: u64,
    s: String,
}

impl Stashable for StructA {
    fn stash(&self, stasher: &mut Stasher) {
        stasher.i32(self.i);
        stasher.u64(self.x);
        stasher.string(&self.s);
    }
}

impl Unstashable for StructA {
    fn unstash(unstasher: &mut Unstasher) -> Result<Self, UnstashError> {
        Ok(StructA {
            i: unstasher.i32()?,
            x: unstasher.u64()?,
            s: unstasher.string()?,
        })
    }
}

impl UnstashableInplace for StructA {
    fn unstash_inplace(&mut self, unstasher: &mut InplaceUnstasher) -> Result<(), UnstashError> {
        unstasher.i32(&mut self.i)?;
        unstasher.u64(&mut self.x)?;
        unstasher.string(&mut self.s)?;
        Ok(())
    }
}

#[test]
fn test_basic_struct() {
    let stash = Stash::new();

    assert_eq!(stash.num_objects(), 0);

    let s1 = StructA {
        i: 123,
        x: 0x0123456789abcdef,
        s: "abcde".to_string(),
    };

    let handle = stash.stash(&s1);

    assert_eq!(stash.num_objects(), 1);

    let s2 = stash.unstash(&handle).unwrap();

    assert_eq!(stash.num_objects(), 1);

    assert_eq!(s2.i, 123);
    assert_eq!(s2.x, 0x0123456789abcdef);
    assert_eq!(s2.s, "abcde");

    let s3 = stash.unstash(&handle).unwrap();

    assert_eq!(stash.num_objects(), 1);

    assert_eq!(s3.i, 123);
    assert_eq!(s3.x, 0x0123456789abcdef);
    assert_eq!(s3.s, "abcde");

    std::mem::drop(handle);

    assert_eq!(stash.num_objects(), 0);
}

#[test]
fn test_basic_struct_changing() {
    let stash = Stash::new();

    assert_eq!(stash.num_objects(), 0);

    let mut s1 = StructA {
        i: 123,
        x: 0x0123456789abcdef,
        s: "abcde".to_string(),
    };

    let handle1 = stash.stash(&s1);

    assert_eq!(stash.num_objects(), 1);

    assert_eq!(handle1.reference_count(), 1);

    s1.i += 1;

    let handle2 = stash.stash(&s1);

    assert_eq!(handle1.reference_count(), 1);
    assert_eq!(handle2.reference_count(), 1);
    assert_ne!(handle1.object_hash(), handle2.object_hash());

    assert_eq!(stash.num_objects(), 2);

    s1.i -= 1;

    let handle3 = stash.stash(&s1);

    assert_eq!(handle1.reference_count(), 2);
    assert_eq!(handle2.reference_count(), 1);
    assert_eq!(handle3.reference_count(), 2);
    assert_ne!(handle1.object_hash(), handle2.object_hash());
    assert_eq!(handle1.object_hash(), handle3.object_hash());
    assert_ne!(handle2.object_hash(), handle3.object_hash());

    assert_eq!(stash.num_objects(), 2); // not 3; contents should match first stash

    let unstashed_1 = stash.unstash(&handle1).unwrap();

    assert_eq!(unstashed_1.i, 123);
    assert_eq!(unstashed_1.x, 0x0123456789abcdef);
    assert_eq!(unstashed_1.s, "abcde");

    assert_eq!(handle1.reference_count(), 2);
    assert_eq!(handle2.reference_count(), 1);
    assert_eq!(handle3.reference_count(), 2);

    let unstashed_2 = stash.unstash(&handle2).unwrap();

    assert_eq!(unstashed_2.i, 124);
    assert_eq!(unstashed_2.x, 0x0123456789abcdef);
    assert_eq!(unstashed_2.s, "abcde");

    assert_eq!(handle1.reference_count(), 2);
    assert_eq!(handle2.reference_count(), 1);
    assert_eq!(handle3.reference_count(), 2);

    let unstashed_3 = stash.unstash(&handle3).unwrap();

    assert_eq!(unstashed_3.i, 123);
    assert_eq!(unstashed_3.x, 0x0123456789abcdef);
    assert_eq!(unstashed_3.s, "abcde");

    assert_eq!(handle1.reference_count(), 2);
    assert_eq!(handle2.reference_count(), 1);
    assert_eq!(handle3.reference_count(), 2);

    assert_eq!(stash.num_objects(), 2);

    std::mem::drop(handle1);

    assert_eq!(handle2.reference_count(), 1);
    assert_eq!(handle3.reference_count(), 1);

    assert_eq!(stash.num_objects(), 2); // handle1 == handle3

    std::mem::drop(handle2);

    assert_eq!(stash.num_objects(), 1);

    assert_eq!(handle3.reference_count(), 1);

    std::mem::drop(handle3);

    assert_eq!(stash.num_objects(), 0);
}

struct StructAProxy(StructA);

impl Stashable for StructAProxy {
    fn stash(&self, stasher: &mut Stasher) {
        stasher.object_proxy(|stasher| {
            stasher.i32(self.0.i);
            stasher.u64(self.0.x);
            stasher.string(&self.0.s);
        });
    }
}

impl Unstashable for StructAProxy {
    fn unstash(unstasher: &mut Unstasher) -> Result<Self, UnstashError> {
        unstasher.object_proxy(|unstasher| {
            Ok(StructAProxy(StructA {
                i: unstasher.i32()?,
                x: unstasher.u64()?,
                s: unstasher.string()?,
            }))
        })
    }
}

#[test]
fn test_basic_struct_proxy() {
    let stash = Stash::new();

    assert_eq!(stash.num_objects(), 0);

    let s1 = StructAProxy(StructA {
        i: 123,
        x: 0x0123456789abcdef,
        s: "abcde".to_string(),
    });

    let handle = stash.stash(&s1);

    // 1 StructAProxy and 1 StructA
    assert_eq!(stash.num_objects(), 2);

    let s2 = stash.unstash(&handle).unwrap();

    assert_eq!(stash.num_objects(), 2);

    assert_eq!(s2.0.i, 123);
    assert_eq!(s2.0.x, 0x0123456789abcdef);
    assert_eq!(s2.0.s, "abcde");

    let s3 = stash.unstash(&handle).unwrap();

    assert_eq!(stash.num_objects(), 2);

    assert_eq!(s3.0.i, 123);
    assert_eq!(s3.0.x, 0x0123456789abcdef);
    assert_eq!(s3.0.s, "abcde");

    std::mem::drop(handle);

    assert_eq!(stash.num_objects(), 0);
}

#[derive(Clone, Eq, PartialEq, Debug)]
struct StructB {
    a1: StructA,
    b: bool,
    a2: StructA,
    u: u8,
    a3: StructA,
}

impl Stashable for StructB {
    fn stash(&self, stasher: &mut Stasher) {
        stasher.object(&self.a1);
        stasher.bool(self.b);
        stasher.object(&self.a2);
        stasher.u8(self.u);
        stasher.object(&self.a3);
    }
}

impl Unstashable for StructB {
    fn unstash(unstasher: &mut Unstasher) -> Result<Self, UnstashError> {
        Ok(StructB {
            a1: unstasher.object()?,
            b: unstasher.bool()?,
            a2: unstasher.object()?,
            u: unstasher.u8()?,
            a3: unstasher.object()?,
        })
    }
}

impl UnstashableInplace for StructB {
    fn unstash_inplace(&mut self, unstasher: &mut InplaceUnstasher) -> Result<(), UnstashError> {
        unstasher.object_inplace(&mut self.a1)?;
        unstasher.bool(&mut self.b)?;
        unstasher.object_inplace(&mut self.a2)?;
        unstasher.u8(&mut self.u)?;
        unstasher.object_inplace(&mut self.a3)?;
        Ok(())
    }
}

#[test]
fn test_one_level_nested_struct() {
    let stash = Stash::new();

    let b1 = StructB {
        a1: StructA {
            i: 1,
            x: 0x0123456789abcdef,
            s: "a".to_string(),
        },
        b: true,
        a2: StructA {
            i: 2,
            x: 0x0123456789abcdef,
            s: "b".to_string(),
        },
        u: 11,
        a3: StructA {
            i: 3,
            x: 0x0123456789abcdef,
            s: "c".to_string(),
        },
    };

    let handle1 = stash.stash(&b1);

    // one B and three A's
    assert_eq!(stash.num_objects(), 4);
    assert_eq!(handle1.reference_count(), 1);

    let b2 = b1.clone();

    let handle2 = stash.stash(&b2);

    // same
    assert_eq!(stash.num_objects(), 4);

    assert_eq!(handle1.reference_count(), 2);
    assert_eq!(handle2.reference_count(), 2);
    assert_eq!(handle1.object_hash(), handle2.object_hash());

    let mut b3 = b1.clone();
    b3.a1.i = 99;
    b3.a2 = b3.a1.clone();
    b3.a3 = b3.a1.clone();

    let handle3 = stash.stash(&b3);

    assert_ne!(handle3.object_hash(), handle1.object_hash());
    assert_eq!(handle1.reference_count(), 2);
    assert_eq!(handle2.reference_count(), 2);
    assert_eq!(handle3.reference_count(), 1);

    // one new B and one new A, copied three times
    assert_eq!(stash.num_objects(), 6);

    std::mem::drop(handle2);

    assert_eq!(stash.num_objects(), 6);
    assert_eq!(handle1.reference_count(), 1);
    assert_eq!(handle3.reference_count(), 1);

    std::mem::drop(handle1);

    assert_eq!(stash.num_objects(), 2);
    assert_eq!(handle3.reference_count(), 1);

    let unstashed3 = stash.unstash(&handle3).unwrap();

    assert_eq!(handle3.reference_count(), 1);

    assert_eq!(unstashed3.a1.i, 99);
    assert_eq!(unstashed3.a2.i, 99);
    assert_eq!(unstashed3.a3.i, 99);

    std::mem::drop(handle3);

    assert_eq!(stash.num_objects(), 0);
}

#[test]
fn test_roundtrip_nested() {
    let create_a = || StructA {
        i: 123,
        x: 0x0123456789abcdef,
        s: "abcde".to_string(),
    };

    let modify_a_i = |s: &mut StructA| {
        s.i += 1;
    };
    let modify_a_x = |s: &mut StructA| {
        s.x = 0x4321;
    };
    let modify_a_s = |s: &mut StructA| {
        s.s.push('z');
    };

    assert_eq!(test_stash_roundtrip(create_a, modify_a_i), Ok(()));
    assert_eq!(test_stash_roundtrip(create_a, modify_a_x), Ok(()));
    assert_eq!(test_stash_roundtrip(create_a, modify_a_s), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(create_a, modify_a_i), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(create_a, modify_a_x), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(create_a, modify_a_s), Ok(()));

    let make_b = || StructB {
        a1: StructA {
            i: 1,
            x: 0x0123456789abcdef,
            s: "a".to_string(),
        },
        b: true,
        a2: StructA {
            i: 2,
            x: 0x0123456789abcdef,
            s: "b".to_string(),
        },
        u: 11,
        a3: StructA {
            i: 3,
            x: 0x0123456789abcdef,
            s: "c".to_string(),
        },
    };

    let modify_b_b = |s: &mut StructB| {
        s.b = !s.b;
    };
    let modify_b_u = |s: &mut StructB| {
        s.u += 2;
    };
    let modify_b_a1 = |s: &mut StructB| s.a1.i += 1;
    let modify_b_a2 = |s: &mut StructB| s.a2.x ^= 0b101;
    let modify_b_a3 = |s: &mut StructB| s.a3.s.push_str("blah");

    assert_eq!(test_stash_roundtrip(make_b, modify_b_b), Ok(()));
    assert_eq!(test_stash_roundtrip(make_b, modify_b_u), Ok(()));
    assert_eq!(test_stash_roundtrip(make_b, modify_b_a1), Ok(()));
    assert_eq!(test_stash_roundtrip(make_b, modify_b_a2), Ok(()));
    assert_eq!(test_stash_roundtrip(make_b, modify_b_a3), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(make_b, modify_b_b), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(make_b, modify_b_u), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(make_b, modify_b_a1), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(make_b, modify_b_a2), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(make_b, modify_b_a3), Ok(()));
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StructWithVecs {
    vec_i32: Vec<i32>,
    vec_u8: Vec<u8>,
}

impl Stashable for StructWithVecs {
    fn stash(&self, stasher: &mut Stasher) {
        stasher.array_of_i32_slice(&self.vec_i32);
        stasher.array_of_u8_iter(self.vec_u8.iter().cloned());
    }
}

impl Unstashable for StructWithVecs {
    fn unstash(unstasher: &mut Unstasher) -> Result<Self, UnstashError> {
        let vec_i32 = unstasher.array_of_i32_iter()?.collect();
        let vec_u8 = unstasher.array_of_u8_vec()?;
        Ok(StructWithVecs { vec_i32, vec_u8 })
    }
}

impl UnstashableInplace for StructWithVecs {
    fn unstash_inplace(&mut self, unstasher: &mut InplaceUnstasher) -> Result<(), UnstashError> {
        unstasher.array_of_i32_vec(&mut self.vec_i32)?;
        unstasher.array_of_u8_vec(&mut self.vec_u8)?;
        Ok(())
    }
}

#[test]
fn test_struct_with_vec() {
    let s1 = StructWithVecs {
        vec_i32: vec![0, 1, 2],
        vec_u8: vec![9, 8, 7, 6, 5],
    };

    let stash = Stash::new();

    let handle = stash.stash(&s1);

    let s2 = stash.unstash(&handle).unwrap();

    assert_eq!(s1, s2);

    assert_eq!(stash.num_objects(), 1);

    std::mem::drop(handle);

    assert_eq!(stash.num_objects(), 0);
}

#[test]
fn test_roundtrip_struct_with_vecs() {
    let create_1 = || StructWithVecs {
        vec_i32: vec![1],
        vec_u8: vec![],
    };
    let create_2 = || StructWithVecs {
        vec_i32: vec![0, 1, 2, 3],
        vec_u8: vec![4, 5, 6, 7],
    };
    let create_3 = || StructWithVecs {
        vec_i32: vec![1001, 1002, 1003],
        vec_u8: vec![],
    };

    let modify_1 = |s: &mut StructWithVecs| s.vec_i32.clear();
    let modify_2 = |s: &mut StructWithVecs| s.vec_i32.push(99);
    let modify_3 = |s: &mut StructWithVecs| s.vec_u8.extend_from_slice(&[1, 2, 3]);

    assert_eq!(test_stash_roundtrip(create_1, modify_1), Ok(()));
    assert_eq!(test_stash_roundtrip(create_1, modify_2), Ok(()));
    assert_eq!(test_stash_roundtrip(create_1, modify_3), Ok(()));
    assert_eq!(test_stash_roundtrip(create_2, modify_1), Ok(()));
    assert_eq!(test_stash_roundtrip(create_2, modify_2), Ok(()));
    assert_eq!(test_stash_roundtrip(create_2, modify_3), Ok(()));
    assert_eq!(test_stash_roundtrip(create_3, modify_1), Ok(()));
    assert_eq!(test_stash_roundtrip(create_3, modify_2), Ok(()));
    assert_eq!(test_stash_roundtrip(create_3, modify_3), Ok(()));

    assert_eq!(test_stash_roundtrip_inplace(create_1, modify_1), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(create_1, modify_2), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(create_1, modify_3), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(create_2, modify_1), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(create_2, modify_2), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(create_2, modify_3), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(create_3, modify_1), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(create_3, modify_2), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(create_3, modify_3), Ok(()));
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StructWithVecOfObjects {
    objects: Vec<StructA>,
}

impl Stashable for StructWithVecOfObjects {
    fn stash(&self, stasher: &mut Stasher) {
        stasher.array_of_objects_slice(&self.objects, Order::Ordered);
    }
}

impl Unstashable for StructWithVecOfObjects {
    fn unstash(unstasher: &mut Unstasher) -> Result<Self, UnstashError> {
        Ok(StructWithVecOfObjects {
            objects: unstasher.array_of_objects_vec()?,
        })
    }
}

impl UnstashableInplace for StructWithVecOfObjects {
    fn unstash_inplace(&mut self, unstasher: &mut InplaceUnstasher) -> Result<(), UnstashError> {
        unstasher.array_of_objects_vec(&mut self.objects)?;
        Ok(())
    }
}

#[test]
fn test_vec_of_objects() {
    let a1 = StructA {
        i: 1,
        x: 0x202,
        s: "abc".to_string(),
    };
    let a2 = StructA {
        i: 2,
        x: 0x404,
        s: "defg".to_string(),
    };
    let a3 = StructA {
        i: 3,
        x: 0x808,
        s: "hijkl".to_string(),
    };

    let s1 = StructWithVecOfObjects {
        objects: vec![
            a1.clone(),
            a2.clone(),
            a2.clone(),
            a3.clone(),
            a3.clone(),
            a3.clone(),
        ],
    };

    let stash = Stash::new();

    let handle_s = stash.stash(&s1);

    assert_eq!(stash.num_objects(), 4);

    let s2 = stash.unstash(&handle_s).unwrap();

    assert_eq!(s1, s2);

    let handle_a1 = stash.stash(&a1);
    let handle_a2 = stash.stash(&a2);
    let handle_a3 = stash.stash(&a3);

    assert_eq!(stash.num_objects(), 4);

    assert_eq!(handle_a1.reference_count(), 2);
    assert_eq!(handle_a2.reference_count(), 3);
    assert_eq!(handle_a3.reference_count(), 4);

    std::mem::drop(handle_s);

    assert_eq!(stash.num_objects(), 3);

    assert_eq!(handle_a1.reference_count(), 1);
    assert_eq!(handle_a2.reference_count(), 1);
    assert_eq!(handle_a3.reference_count(), 1);

    std::mem::drop(handle_a1);
    std::mem::drop(handle_a2);
    std::mem::drop(handle_a3);

    assert_eq!(stash.num_objects(), 0);
}

#[test]
fn test_roundtrip_vec_of_objects() {
    let a1 = StructA {
        i: 1,
        x: 0x202,
        s: "abc".to_string(),
    };
    let a2 = StructA {
        i: 2,
        x: 0x404,
        s: "defg".to_string(),
    };
    let a3 = StructA {
        i: 3,
        x: 0x808,
        s: "hijkl".to_string(),
    };

    let create_1 = || StructWithVecOfObjects { objects: vec![] };
    let create_2 = || StructWithVecOfObjects {
        objects: vec![a1.clone()],
    };
    let create_3 = || StructWithVecOfObjects {
        objects: vec![a1.clone(), a2.clone(), a3.clone()],
    };
    let create_4 = || StructWithVecOfObjects {
        objects: vec![
            a1.clone(),
            a2.clone(),
            a2.clone(),
            a3.clone(),
            a3.clone(),
            a3.clone(),
        ],
    };

    let modify_1 = |s: &mut StructWithVecOfObjects| {
        if s.objects.is_empty() {
            s.objects.push(a1.clone());
        } else {
            s.objects.clear();
        }
    };
    let modify_2 = |s: &mut StructWithVecOfObjects| {
        s.objects.push(a2.clone());
        s.objects.reverse();
    };

    assert_eq!(test_stash_roundtrip(create_1, modify_1), Ok(()));
    assert_eq!(test_stash_roundtrip(create_2, modify_1), Ok(()));
    assert_eq!(test_stash_roundtrip(create_3, modify_1), Ok(()));
    assert_eq!(test_stash_roundtrip(create_4, modify_1), Ok(()));
    assert_eq!(test_stash_roundtrip(create_1, modify_2), Ok(()));
    assert_eq!(test_stash_roundtrip(create_2, modify_2), Ok(()));
    assert_eq!(test_stash_roundtrip(create_3, modify_2), Ok(()));
    assert_eq!(test_stash_roundtrip(create_4, modify_2), Ok(()));

    assert_eq!(test_stash_roundtrip_inplace(create_1, modify_1), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(create_2, modify_1), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(create_3, modify_1), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(create_4, modify_1), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(create_1, modify_2), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(create_2, modify_2), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(create_3, modify_2), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(create_4, modify_2), Ok(()));
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct StructWithHashSetOfBasicObjects {
    objects: HashSet<StructA>,
}

impl Stashable for StructWithHashSetOfBasicObjects {
    fn stash(&self, stasher: &mut Stasher) {
        stasher.array_of_objects_iter(self.objects.iter(), Order::Unordered);
    }
}

impl Unstashable for StructWithHashSetOfBasicObjects {
    fn unstash(unstasher: &mut Unstasher) -> Result<Self, UnstashError> {
        Ok(StructWithHashSetOfBasicObjects {
            objects: unstasher
                .array_of_objects_iter()?
                .collect::<Result<_, _>>()?,
        })
    }
}

impl UnstashableInplace for StructWithHashSetOfBasicObjects {
    fn unstash_inplace(&mut self, unstasher: &mut InplaceUnstasher) -> Result<(), UnstashError> {
        let mut temp_vec = Vec::<StructA>::new();
        unstasher.array_of_objects_vec::<StructA>(&mut temp_vec)?;
        if unstasher.phase() == InplaceUnstashPhase::Write {
            self.objects = temp_vec.into_iter().collect();
        }
        Ok(())
    }
}

#[test]
fn test_hashset_of_basic_objects() {
    let mut objects = HashSet::new();

    objects.insert(StructA {
        i: 1,
        x: 0x202,
        s: "abc".to_string(),
    });
    objects.insert(StructA {
        i: 2,
        x: 0x404,
        s: "defg".to_string(),
    });
    objects.insert(StructA {
        i: 3,
        x: 0x808,
        s: "hijkl".to_string(),
    });

    let s1 = StructWithHashSetOfBasicObjects { objects };

    let stash = Stash::new();

    let handle = stash.stash(&s1);

    let s2 = stash.unstash(&handle).unwrap();

    assert_eq!(s1, s2);
}

#[test]
fn test_roundtrip_hashset_of_basic_objects() {
    let create = || {
        let mut objects = HashSet::new();

        for i in 5..50 {
            objects.insert(StructA {
                i,
                x: 44,
                s: "four hundred and forty four".to_string(),
            });
        }
        StructWithHashSetOfBasicObjects { objects }
    };

    let modify_1 = |s: &mut StructWithHashSetOfBasicObjects| {
        s.objects.insert(StructA {
            i: 1,
            x: 0x202,
            s: "abc".to_string(),
        });
    };
    let modify_2 = |s: &mut StructWithHashSetOfBasicObjects| {
        s.objects.insert(StructA {
            i: 2,
            x: 0x404,
            s: "defg".to_string(),
        });
        s.objects.insert(StructA {
            i: 3,
            x: 0x808,
            s: "hijkl".to_string(),
        });
    };
    let modify_3 = |s: &mut StructWithHashSetOfBasicObjects| {
        s.objects.retain(|a| a.i % 2 == 0);
    };

    assert_eq!(test_stash_roundtrip(create, modify_1), Ok(()));
    assert_eq!(test_stash_roundtrip(create, modify_2), Ok(()));
    assert_eq!(test_stash_roundtrip(create, modify_3), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(create, modify_1), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(create, modify_2), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(create, modify_3), Ok(()));
}

struct WeirdContainer<T> {
    items: Vec<Option<Box<T>>>,
}

impl<T> WeirdContainer<T> {
    fn new(capacity: usize) -> WeirdContainer<T> {
        let mut items = Vec::new();
        items.resize_with(capacity, || None);
        WeirdContainer { items }
    }

    fn items<'a>(&'a self) -> impl 'a + Iterator<Item = &T> {
        self.items.iter().filter_map(|i| match i {
            Some(i) => Some(&**i),
            None => None,
        })
    }

    fn clear(&mut self) {
        for item in &mut self.items {
            *item = None;
        }
    }

    fn insert_somewhere_random(&mut self, item: T) {
        let item = Box::new(item);
        let n = self.items.len();
        let idx = thread_rng().gen_range(0..n);
        for probe_idx in 0..n {
            let slot = &mut self.items[(idx + probe_idx) % n];
            if slot.is_none() {
                *slot = Some(item);
                return;
            }
        }
        panic!("WeirdContainer overflow");
    }

    fn scramble(&mut self) {
        self.items.shuffle(&mut thread_rng());
    }

    fn foreach_mut<F: FnMut(&mut T)>(&mut self, mut f: F) {
        for item in &mut self.items {
            if let Some(item) = item.as_mut() {
                f(item);
            }
        }
    }
}

struct StructWithWeirdContainer {
    container: WeirdContainer<StructA>,
}

impl Stashable for StructWithWeirdContainer {
    fn stash(&self, stasher: &mut Stasher) {
        stasher.array_of_objects_iter(self.container.items(), Order::Unordered);
    }
}

impl Unstashable for StructWithWeirdContainer {
    fn unstash(unstasher: &mut Unstasher) -> Result<Self, UnstashError> {
        let mut container = WeirdContainer::<StructA>::new(1024);
        for s in unstasher.array_of_objects_iter::<StructA>()? {
            container.insert_somewhere_random(s?);
        }
        Ok(StructWithWeirdContainer { container })
    }
}

impl UnstashableInplace for StructWithWeirdContainer {
    fn unstash_inplace(&mut self, unstasher: &mut InplaceUnstasher) -> Result<(), UnstashError> {
        let mut temp_objects: Vec<StructA> = Vec::new();
        unstasher.array_of_objects_vec(&mut temp_objects)?;
        if unstasher.phase() == InplaceUnstashPhase::Write {
            self.container.clear();
            for object in temp_objects {
                self.container.insert_somewhere_random(object);
            }
        }
        Ok(())
    }
}

#[test]
fn test_roundtrip_weird_container() {
    let create = || {
        let mut container = WeirdContainer::<StructA>::new(1024);
        container.insert_somewhere_random(StructA {
            i: 1,
            x: 2,
            s: "three".to_string(),
        });
        container.insert_somewhere_random(StructA {
            i: 9,
            x: 21,
            s: "threee".to_string(),
        });
        container.insert_somewhere_random(StructA {
            i: 909,
            x: 42,
            s: "threeeeeeeee".to_string(),
        });
        StructWithWeirdContainer { container }
    };

    let modify_1 = |s: &mut StructWithWeirdContainer| {
        s.container.insert_somewhere_random(StructA {
            i: 4,
            x: 44,
            s: "four hundred and forty four".to_string(),
        });
    };
    let modify_2 = |s: &mut StructWithWeirdContainer| {
        s.container.scramble();
        s.container.insert_somewhere_random(StructA {
            i: 90,
            x: 91,
            s: "dcba".to_string(),
        });
    };
    let modify_3 = |s: &mut StructWithWeirdContainer| {
        s.container.foreach_mut(|a| a.x *= 2);
    };

    assert_eq!(test_stash_roundtrip(create, modify_1), Ok(()));
    assert_eq!(test_stash_roundtrip(create, modify_2), Ok(()));
    assert_eq!(test_stash_roundtrip(create, modify_3), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(create, modify_1), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(create, modify_2), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(create, modify_3), Ok(()));
}

struct GraphNode {
    id: i32,
    data: Vec<u8>,
    inputs: Vec<i32>,
}

impl GraphNode {
    fn new(id: i32, data: Vec<u8>) -> GraphNode {
        GraphNode {
            id,
            data,
            inputs: Vec::new(),
        }
    }

    fn set_data(&mut self, data: Vec<u8>) {
        self.data = data;
    }
}

struct Graph {
    nodes: HashMap<i32, GraphNode>,
}

impl Graph {
    fn new() -> Graph {
        Graph {
            nodes: HashMap::new(),
        }
    }

    fn add_node(&mut self, id: i32, data: Vec<u8>) {
        self.nodes.insert(id, GraphNode::new(id, data));
    }

    fn remove_node(&mut self, id: i32) {
        self.nodes.remove(&id).unwrap();
    }

    fn node_mut(&mut self, id: i32) -> Option<&mut GraphNode> {
        self.nodes.get_mut(&id)
    }

    fn connect_nodes(&mut self, src: i32, dst: i32) {
        self.nodes.get_mut(&src).unwrap().inputs.push(dst);
    }

    fn disconnect_node(&mut self, id: i32) {
        self.nodes.get_mut(&id).unwrap().inputs.clear();
    }

    fn node_ids(&self) -> Vec<i32> {
        self.nodes.keys().cloned().collect()
    }
}

impl Stashable for Graph {
    fn stash(&self, stasher: &mut Stasher) {
        // nodes
        stasher.array_of_proxy_objects(
            self.nodes.values(),
            |node, stasher| {
                stasher.i32(node.id);
                stasher.array_of_u8_slice(&node.data);
            },
            Order::Unordered,
        );

        // connections
        // (these could also be serialized with each node,
        // but it proves a more interesting point to do it
        // separately, which is support for different APIs
        // that require separate explicit actions for stuff)
        let connect_src_dst_pairs = self
            .nodes
            .values()
            .map(|node| {
                node.inputs
                    .iter()
                    .map(|dst| -> [i32; 2] { [node.id, *dst] })
            })
            .flatten();

        stasher.array_of_proxy_objects(
            connect_src_dst_pairs,
            |[src, dst], stasher| {
                stasher.i32(*src);
                stasher.i32(*dst);
            },
            Order::Unordered,
        );
    }
}

impl Unstashable for Graph {
    fn unstash(unstasher: &mut Unstasher) -> Result<Self, UnstashError> {
        let mut graph = Graph::new();

        unstasher.array_of_proxy_objects(|unstasher| {
            let id = unstasher.i32()?;
            let data = unstasher.array_of_u8_vec()?;
            graph.add_node(id, data);
            Ok(())
        })?;

        unstasher.array_of_proxy_objects(|unstasher| {
            let src = unstasher.i32()?;
            let dst = unstasher.i32()?;
            graph.connect_nodes(src, dst);
            Ok(())
        })?;

        Ok(graph)
    }
}

impl UnstashableInplace for Graph {
    fn unstash_inplace(&mut self, unstasher: &mut InplaceUnstasher) -> Result<(), UnstashError> {
        let phase = unstasher.phase();

        let mut node_ids_to_keep = Vec::<i32>::new();

        unstasher.array_of_proxy_objects(|u| {
            let id = u.i32()?;
            let data = u.array_of_u8_vec()?;

            if phase == InplaceUnstashPhase::Write {
                if let Some(node) = self.node_mut(id) {
                    // Preserve existing nodes with matching ids
                    node.set_data(data);
                } else {
                    // Add new nodes as needed
                    self.add_node(id, data);
                }

                node_ids_to_keep.push(id);
            }

            Ok(())
        })?;

        // Remove unreferenced nodes
        if phase == InplaceUnstashPhase::Write {
            for id in self.node_ids() {
                if !node_ids_to_keep.contains(&id) {
                    self.remove_node(id);
                }
            }
        }

        // Clear all connections
        if phase == InplaceUnstashPhase::Write {
            for id in self.node_ids() {
                self.disconnect_node(id);
            }
        }

        // Add back unstashed connections
        unstasher.array_of_proxy_objects(|u| {
            let src = u.i32()?;
            let dst = u.i32()?;

            if phase == InplaceUnstashPhase::Write {
                self.connect_nodes(src, dst);
            }

            Ok(())
        })?;

        Ok(())
    }
}

#[test]
fn test_graph_roundtrip() {
    let create_1 = || Graph::new();
    let create_2 = || {
        let mut graph = Graph::new();
        graph.add_node(1, vec![]);
        graph.add_node(2, vec![0x0]);
        graph
    };
    let create_3 = || {
        let mut graph = Graph::new();
        graph.add_node(1, vec![]);
        graph.add_node(2, vec![0x0]);
        graph.connect_nodes(1, 2);
        graph.connect_nodes(2, 2);
        graph
    };

    let modify_1 = |graph: &mut Graph| {
        graph.add_node(100, vec![0xd, 0xe, 0xf]);
    };

    let modify_2 = |graph: &mut Graph| {
        graph.add_node(50, vec![0xf]);
        for i in 51..=60 {
            graph.add_node(i, vec![i as u8]);
            graph.connect_nodes(i, 50);
        }
    };

    assert_eq!(test_stash_roundtrip(create_1, modify_1), Ok(()));
    assert_eq!(test_stash_roundtrip(create_2, modify_1), Ok(()));
    assert_eq!(test_stash_roundtrip(create_3, modify_1), Ok(()));
    assert_eq!(test_stash_roundtrip(create_1, modify_2), Ok(()));
    assert_eq!(test_stash_roundtrip(create_2, modify_2), Ok(()));
    assert_eq!(test_stash_roundtrip(create_3, modify_2), Ok(()));

    assert_eq!(test_stash_roundtrip_inplace(create_1, modify_1), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(create_2, modify_1), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(create_3, modify_1), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(create_1, modify_2), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(create_2, modify_2), Ok(()));
    assert_eq!(test_stash_roundtrip_inplace(create_3, modify_2), Ok(()));
}
