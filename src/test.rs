use std::collections::HashSet;

use crate::{
    test_stash_roundtrip, test_stash_roundtrip_inplace, unstasher::InplaceUnstasher, Stash,
    Stashable, Stasher, UnstashError, Unstashable, UnstashableInplace, Unstasher,
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
        stasher.stashable(&self.a1);
        stasher.bool(self.b);
        stasher.stashable(&self.a2);
        stasher.u8(self.u);
        stasher.stashable(&self.a3);
    }
}

impl Unstashable for StructB {
    fn unstash(unstasher: &mut Unstasher) -> Result<Self, UnstashError> {
        Ok(StructB {
            a1: unstasher.unstash()?,
            b: unstasher.bool()?,
            a2: unstasher.unstash()?,
            u: unstasher.u8()?,
            a3: unstasher.unstash()?,
        })
    }
}

impl UnstashableInplace for StructB {
    fn unstash_inplace(&mut self, unstasher: &mut InplaceUnstasher) -> Result<(), UnstashError> {
        unstasher.unstash_inplace(&mut self.a1)?;
        unstasher.bool(&mut self.b)?;
        unstasher.unstash_inplace(&mut self.a2)?;
        unstasher.u8(&mut self.u)?;
        unstasher.unstash_inplace(&mut self.a3)?;
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
        stasher.array_of_objects_slice(&self.objects);
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
        stasher.array_of_objects_iter(self.objects.iter());
    }
}

impl Unstashable for StructWithHashSetOfBasicObjects {
    fn unstash(unstasher: &mut Unstasher) -> Result<Self, UnstashError> {
        Ok(StructWithHashSetOfBasicObjects {
            objects: unstasher
                .array_of_objects_iter::<StructA>()?
                .collect::<Result<HashSet<StructA>, UnstashError>>()?,
        })
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
