use crate::{Stash, Stashable, Stasher, Unstashable, Unstasher};

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
    fn unstash(unstasher: &mut Unstasher) -> Result<Self, ()> {
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

    let s1 = StructA {
        i: 123,
        x: 0.125,
        s: "abcde".to_string(),
    };

    let handle = stash.stash(&s1);

    let s2 = stash.unstash(&handle).unwrap();

    assert_eq!(s2.i, 123);
    assert_eq!(s2.x, 0.125);
    assert_eq!(s2.s, "abcde");
}
