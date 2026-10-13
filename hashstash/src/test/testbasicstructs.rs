use crate::{
    test_stash_roundtrip, test_stash_roundtrip_inplace, InplaceUnstasher, Stash, Stashable,
    Stasher, UnstashError, Unstashable, UnstashableInplace, Unstasher,
};

#[derive(Clone, Eq, PartialEq, Debug, Hash)]
pub(super) struct StructA {
    pub(super) i: i32,
    pub(super) x: u64,
    pub(super) s: String,
}

impl<C: Copy> Stashable<C> for StructA {
    fn stash(&self, stasher: &mut Stasher<C>) {
        stasher.i32(self.i);
        stasher.u64(self.x);
        stasher.string(&self.s);
    }
}

impl<C: Copy> Unstashable<C> for StructA {
    fn unstash(unstasher: &mut Unstasher<C>) -> Result<Self, UnstashError> {
        Ok(StructA {
            i: unstasher.i32()?,
            x: unstasher.u64()?,
            s: unstasher.string()?,
        })
    }
}

impl<C: Copy> UnstashableInplace<C> for StructA {
    fn unstash_inplace(&mut self, unstasher: &mut InplaceUnstasher<C>) -> Result<(), UnstashError> {
        unstasher.i32_inplace(&mut self.i)?;
        unstasher.u64_inplace(&mut self.x)?;
        unstasher.string_inplace(&mut self.s)?;
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

struct StructAWrapper(StructA);

impl Stashable for StructAWrapper {
    fn stash(&self, stasher: &mut Stasher) {
        stasher.object_proxy(|stasher| {
            stasher.i32(self.0.i);
            stasher.u64(self.0.x);
            stasher.string(&self.0.s);
        });
    }
}

impl Unstashable for StructAWrapper {
    fn unstash(unstasher: &mut Unstasher) -> Result<Self, UnstashError> {
        unstasher.object_proxy(|unstasher| {
            Ok(StructAWrapper(StructA {
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

    let s1 = StructAWrapper(StructA {
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
pub(super) struct StructB {
    pub(super) a1: StructA,
    pub(super) b: bool,
    pub(super) a2: StructA,
    pub(super) u: u8,
    pub(super) a3: StructA,
}

impl<C: Copy> Stashable<C> for StructB {
    fn stash(&self, stasher: &mut Stasher<C>) {
        stasher.object(&self.a1);
        stasher.bool(self.b);
        stasher.object(&self.a2);
        stasher.u8(self.u);
        stasher.object(&self.a3);
    }
}

impl<C: Copy> Unstashable<C> for StructB {
    fn unstash(unstasher: &mut Unstasher<C>) -> Result<Self, UnstashError> {
        Ok(StructB {
            a1: unstasher.object()?,
            b: unstasher.bool()?,
            a2: unstasher.object()?,
            u: unstasher.u8()?,
            a3: unstasher.object()?,
        })
    }
}

impl<C: Copy> UnstashableInplace<C> for StructB {
    fn unstash_inplace(&mut self, unstasher: &mut InplaceUnstasher<C>) -> Result<(), UnstashError> {
        unstasher.object_inplace(&mut self.a1)?;
        unstasher.bool_inplace(&mut self.b)?;
        unstasher.object_inplace(&mut self.a2)?;
        unstasher.u8_inplace(&mut self.u)?;
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

    assert_eq!(test_stash_roundtrip(create_a, modify_a_i, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(create_a, modify_a_x, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(create_a, modify_a_s, (), ()), Ok(()));
    assert_eq!(
        test_stash_roundtrip_inplace(create_a, modify_a_i, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(create_a, modify_a_x, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(create_a, modify_a_s, (), ()),
        Ok(())
    );

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

    assert_eq!(test_stash_roundtrip(make_b, modify_b_b, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(make_b, modify_b_u, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(make_b, modify_b_a1, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(make_b, modify_b_a2, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(make_b, modify_b_a3, (), ()), Ok(()));
    assert_eq!(
        test_stash_roundtrip_inplace(make_b, modify_b_b, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(make_b, modify_b_u, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(make_b, modify_b_a1, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(make_b, modify_b_a2, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(make_b, modify_b_a3, (), ()),
        Ok(())
    );
}
