use std::collections::HashSet;

use crate::{
    test::testbasicstructs::StructA, test_stash_roundtrip, test_stash_roundtrip_inplace,
    InplaceUnstasher, Order, Stash, Stashable, Stasher, UnstashError, Unstashable,
    UnstashableInplace, Unstasher,
};

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
        unstasher.array_of_i32_vec_inplace(&mut self.vec_i32)?;
        unstasher.array_of_u8_vec_inplace(&mut self.vec_u8)?;
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

    assert_eq!(test_stash_roundtrip(create_1, modify_1, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(create_1, modify_2, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(create_1, modify_3, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(create_2, modify_1, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(create_2, modify_2, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(create_2, modify_3, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(create_3, modify_1, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(create_3, modify_2, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(create_3, modify_3, (), ()), Ok(()));

    assert_eq!(
        test_stash_roundtrip_inplace(create_1, modify_1, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(create_1, modify_2, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(create_1, modify_3, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(create_2, modify_1, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(create_2, modify_2, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(create_2, modify_3, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(create_3, modify_1, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(create_3, modify_2, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(create_3, modify_3, (), ()),
        Ok(())
    );
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
    fn unstash_inplace(
        &mut self,
        unstasher: &mut InplaceUnstasher<()>,
    ) -> Result<(), UnstashError> {
        unstasher.array_of_objects_vec_inplace(&mut self.objects)?;
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

    assert_eq!(test_stash_roundtrip(create_1, modify_1, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(create_2, modify_1, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(create_3, modify_1, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(create_4, modify_1, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(create_1, modify_2, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(create_2, modify_2, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(create_3, modify_2, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(create_4, modify_2, (), ()), Ok(()));

    assert_eq!(
        test_stash_roundtrip_inplace(create_1, modify_1, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(create_2, modify_1, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(create_3, modify_1, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(create_4, modify_1, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(create_1, modify_2, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(create_2, modify_2, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(create_3, modify_2, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(create_4, modify_2, (), ()),
        Ok(())
    );
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
    fn unstash_inplace(
        &mut self,
        unstasher: &mut InplaceUnstasher<()>,
    ) -> Result<(), UnstashError> {
        let mut temp_vec = Vec::<StructA>::new();
        // NOTE: could also use array_of_proxy_objects but that
        // gets tested elsewhere
        unstasher.array_of_objects_vec_inplace::<StructA>(&mut temp_vec)?;
        if unstasher.time_to_write() {
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

    assert_eq!(test_stash_roundtrip(create, modify_1, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(create, modify_2, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(create, modify_3, (), ()), Ok(()));
    assert_eq!(
        test_stash_roundtrip_inplace(create, modify_1, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(create, modify_2, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(create, modify_3, (), ()),
        Ok(())
    );
}
