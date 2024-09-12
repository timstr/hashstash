use crate::{Stash, Stashable, Stasher, UnstashError, Unstashable, UnstashableInplace, Unstasher};

#[derive(Clone)]
struct StructA {
    i: i32,
    x: f64,
    s: String,
}

impl Stashable for StructA {
    fn stash(&self, stasher: &mut Stasher) {
        stasher.i32(self.i);
        stasher.f64(self.x);
        stasher.string(&self.s);
    }
}

impl Unstashable for StructA {
    fn unstash(unstasher: &mut Unstasher) -> Result<Self, UnstashError> {
        Ok(StructA {
            i: unstasher.i32()?,
            x: unstasher.f64()?,
            s: unstasher.string()?,
        })
    }
}

#[test]
fn test_basic_struct() {
    let stash = Stash::new();

    assert_eq!(stash.num_objects(), 0);

    let s1 = StructA {
        i: 123,
        x: 0.125,
        s: "abcde".to_string(),
    };

    let handle = stash.stash(&s1);

    assert_eq!(stash.num_objects(), 1);

    let s2 = stash.unstash(&handle).unwrap();

    assert_eq!(stash.num_objects(), 1);

    assert_eq!(s2.i, 123);
    assert_eq!(s2.x, 0.125);
    assert_eq!(s2.s, "abcde");

    let s3 = stash.unstash(&handle).unwrap();

    assert_eq!(stash.num_objects(), 1);

    assert_eq!(s3.i, 123);
    assert_eq!(s3.x, 0.125);
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
        x: 0.125,
        s: "abcde".to_string(),
    };

    let handle1 = stash.stash(&s1);

    assert_eq!(stash.num_objects(), 1);

    s1.i += 1;

    let handle2 = stash.stash(&s1);

    assert_eq!(stash.num_objects(), 2);

    s1.i -= 1;

    let handle3 = stash.stash(&s1);

    assert_eq!(stash.num_objects(), 2); // not 3; contents should match first stash

    let unstashed_1 = stash.unstash(&handle1).unwrap();

    assert_eq!(unstashed_1.i, 123);
    assert_eq!(unstashed_1.x, 0.125);
    assert_eq!(unstashed_1.s, "abcde");

    let unstashed_2 = stash.unstash(&handle2).unwrap();

    assert_eq!(unstashed_2.i, 124);
    assert_eq!(unstashed_2.x, 0.125);
    assert_eq!(unstashed_2.s, "abcde");

    let unstashed_3 = stash.unstash(&handle3).unwrap();

    assert_eq!(unstashed_3.i, 123);
    assert_eq!(unstashed_3.x, 0.125);
    assert_eq!(unstashed_3.s, "abcde");

    assert_eq!(stash.num_objects(), 2);

    std::mem::drop(handle1);

    assert_eq!(stash.num_objects(), 2); // handle1 == handle3

    std::mem::drop(handle2);

    assert_eq!(stash.num_objects(), 1);

    std::mem::drop(handle3);

    assert_eq!(stash.num_objects(), 0);
}

#[derive(Clone)]
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
    fn unstash_inplace(&mut self, unstasher: &mut Unstasher) -> Result<(), UnstashError> {
        self.a1 = unstasher.unstash()?;
        self.b = unstasher.bool()?;
        self.a2 = unstasher.unstash()?;
        self.u = unstasher.u8()?;
        self.a3 = unstasher.unstash()?;
        Ok(())
    }
}

#[test]
fn test_one_level_nested_struct() {
    // TODO: test reference counts as well?

    let stash = Stash::new();

    let b1 = StructB {
        a1: StructA {
            i: 1,
            x: 1.5,
            s: "a".to_string(),
        },
        b: true,
        a2: StructA {
            i: 2,
            x: 2.5,
            s: "b".to_string(),
        },
        u: 11,
        a3: StructA {
            i: 3,
            x: 3.5,
            s: "c".to_string(),
        },
    };

    let handle1 = stash.stash(&b1);

    // one B and three A's
    assert_eq!(stash.num_objects(), 4);

    let b2 = b1.clone();

    let handle2 = stash.stash(&b2);

    // same
    assert_eq!(stash.num_objects(), 4);

    let mut b3 = b1.clone();
    b3.a1.i = 99;
    b3.a2 = b3.a1.clone();
    b3.a3 = b3.a1.clone();

    let handle3 = stash.stash(&b3);

    // one new B and one new A, copied three times
    assert_eq!(stash.num_objects(), 6);

    std::mem::drop(handle2);

    assert_eq!(stash.num_objects(), 6);

    std::mem::drop(handle1);

    assert_eq!(stash.num_objects(), 2);

    let unstashed3 = stash.unstash(&handle3).unwrap();

    assert_eq!(unstashed3.a1.i, 99);
    assert_eq!(unstashed3.a2.i, 99);
    assert_eq!(unstashed3.a3.i, 99);

    std::mem::drop(handle3);

    assert_eq!(stash.num_objects(), 0);
}
