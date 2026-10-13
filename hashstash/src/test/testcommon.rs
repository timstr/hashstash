use std::{cell::RefCell, collections::HashMap};

use crate::{test::testbasicstructs::StructA, Stash};

#[test]
fn test_prim() {
    let stash = Stash::new();

    let x1 = 404_i32;

    let h1 = stash.stash(&x1);

    let mut x2 = stash.unstash(&h1).unwrap();

    assert_eq!(x2, x1);

    x2 += 202;

    assert_ne!(x2, x1);

    stash.unstash_inplace(&h1, &mut x2).unwrap();

    assert_eq!(x2, x1);
}

#[test]
fn test_string() {
    let stash = Stash::new();

    let x1 = "abcdef".to_string();

    let h1 = stash.stash(&x1);

    let mut x2 = stash.unstash(&h1).unwrap();

    assert_eq!(x2, x1);

    x2.push('g');

    assert_ne!(x2, x1);

    stash.unstash_inplace(&h1, &mut x2).unwrap();

    assert_eq!(x2, x1);
}

#[test]
fn test_option() {
    let stash = Stash::new();

    let some_123 = Some(StructA {
        i: 123,
        x: 0x0123456789abcdef,
        s: "abcde".to_string(),
    });

    let handle_some_123 = stash.stash(&some_123);

    assert_eq!(some_123, stash.unstash(&handle_some_123).unwrap());

    let some_234 = Some(StructA {
        i: 234,
        x: 0xff00ff00,
        s: "xyz".to_string(),
    });

    let handle_some_234 = stash.stash(&some_234);

    assert_eq!(some_234, stash.unstash(&handle_some_234).unwrap());
    assert_ne!(some_123, stash.unstash(&handle_some_234).unwrap());
    assert_ne!(some_234, stash.unstash(&handle_some_123).unwrap());

    let mut a: Option<StructA> = None;

    let handle_none = stash.stash(&a);

    assert_eq!(None, stash.unstash(&handle_none).unwrap());

    stash.unstash_inplace(&handle_some_123, &mut a).unwrap();

    assert_eq!(a, some_123);

    stash.unstash_inplace(&handle_none, &mut a).unwrap();

    assert_eq!(a, None);

    stash.unstash_inplace(&handle_some_234, &mut a).unwrap();

    assert_eq!(a, some_234);
}

#[test]
fn test_box() {
    let stash = Stash::new();

    let x1 = Box::new(StructA {
        i: 9,
        x: 8,
        s: "seven".into(),
    });

    let h1 = stash.stash(&x1);

    let mut x2 = stash.unstash(&h1).unwrap();

    assert_eq!(x2, x1);

    x2.i = 11;

    assert_ne!(x2, x1);

    stash.unstash_inplace(&h1, &mut x2).unwrap();

    assert_eq!(x2, x1);
}

#[test]
fn test_refcell() {
    let stash = Stash::new();

    let x1 = RefCell::new(StructA {
        i: 9,
        x: 8,
        s: "seven".into(),
    });

    let h1 = stash.stash(&x1);

    let mut x2 = stash.unstash(&h1).unwrap();

    assert_eq!(x2, x1);

    x2.borrow_mut().i = 11;

    assert_ne!(x2, x1);

    stash.unstash_inplace(&h1, &mut x2).unwrap();

    assert_eq!(x2, x1);
}

#[test]
fn test_hashmap() {
    let stash = Stash::new();

    let x1 = {
        let mut m = HashMap::new();
        m.insert(
            "one".to_string(),
            StructA {
                i: 1,
                x: 1,
                s: "1".to_string(),
            },
        );
        m.insert(
            "two".to_string(),
            StructA {
                i: 2,
                x: 2,
                s: "2".to_string(),
            },
        );
        m
    };

    let h1 = stash.stash(&x1);

    let mut x2 = stash.unstash(&h1).unwrap();

    assert_eq!(x2, x1);

    x2.remove("two").unwrap();
    x2.insert(
        "three".to_string(),
        StructA {
            i: 3,
            x: 3,
            s: "3".to_string(),
        },
    );

    assert_ne!(x2, x1);

    stash.unstash_inplace(&h1, &mut x2).unwrap();

    assert_eq!(x2, x1);
}
