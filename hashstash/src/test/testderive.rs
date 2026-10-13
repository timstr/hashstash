use crate::{
    test::testbasicstructs::{StructA, StructB},
    Stash, Stashable, Unstashable, UnstashableInplace,
};

#[derive(Clone, Debug, Eq, PartialEq, Stashable, Unstashable, UnstashableInplace)]
struct StructC {
    i: i32,
    x: u64,
    s: String,
    a: [u8; 8],
    b: StructB,
    vi: Vec<u16>,
    vo: Vec<StructA>,
}

#[test]
fn test_derived_struct_c() {
    let stash = Stash::new();

    let c1 = StructC {
        i: 101,
        x: 90000007,
        s: "fruit basket".to_string(),
        a: 0xfe712d9a4_u64.to_be_bytes(),
        b: StructB {
            a1: StructA {
                i: 1,
                x: 111,
                s: "one".to_string(),
            },
            b: true,
            a2: StructA {
                i: 2,
                x: 222,
                s: "two".to_string(),
            },
            u: 9,
            a3: StructA {
                i: 3,
                x: 333,
                s: "three".to_string(),
            },
        },
        vi: vec![6, 5, 4, 3],
        vo: vec![
            StructA {
                i: 44,
                x: 4004,
                s: "four".to_string(),
            },
            StructA {
                i: 55,
                x: 5005,
                s: "five".to_string(),
            },
        ],
    };

    let handle_c1 = stash.stash(&c1);

    let c2 = stash.unstash(&handle_c1).unwrap();

    assert_eq!(c1, c2);

    let mut c3 = c1.clone();
    c3.x = 80808;
    c3.s = "buns".to_string();
    c3.b.a2 = c3.b.a1.clone();
    c3.b.a3 = c3.b.a1.clone();
    c3.vi = vec![0, 1, 0, 2, 0, 3, 0, 4];
    c3.vo.clear();

    assert_ne!(c1, c3);

    stash.unstash_inplace(&handle_c1, &mut c3).unwrap();

    assert_eq!(c1, c3);
}

#[derive(Debug, Eq, PartialEq, Clone, Stashable, Unstashable, UnstashableInplace)]
struct StructD(i32, u64, String, StructA);

#[test]
fn test_derived_struct_d() {
    let stash = Stash::new();

    let d1 = StructD(
        -11,
        1111,
        "one one one".to_string(),
        StructA {
            i: 505,
            x: 987654321,
            s: "aubergine".to_string(),
        },
    );

    let handle_c1 = stash.stash(&d1);

    let d2 = stash.unstash(&handle_c1).unwrap();

    assert_eq!(d1, d2);

    let mut d3 = d1.clone();
    d3.0 = 12;
    d3.2 = "two two two".to_string();
    d3.3.s = "eggplant".to_string();

    assert_ne!(d1, d3);

    stash.unstash_inplace(&handle_c1, &mut d3).unwrap();

    assert_eq!(d1, d3);
}

#[derive(Clone, Eq, PartialEq, Debug, Stashable, Unstashable, UnstashableInplace)]
enum EnumA {
    One,
    Two(usize),
    // Three(f32, [f64; 2]),
    Four { a: StructA, b: StructB },
}

#[test]
fn test_derived_enum_a() {
    let stash = Stash::new();

    let e1 = EnumA::One;

    let handle_e1 = stash.stash(&e1);

    let e2 = stash.unstash(&handle_e1).unwrap();

    assert_eq!(e1, e2);

    let mut e3 = EnumA::Two(90807);

    assert_ne!(e1, e3);

    stash.unstash_inplace(&handle_e1, &mut e3).unwrap();

    assert_eq!(e1, e3);

    let e4 = EnumA::Four {
        a: StructA {
            i: 1,
            x: 9001,
            s: "one".to_string(),
        },
        b: StructB {
            a1: StructA {
                i: 2,
                x: 9002,
                s: "two".to_string(),
            },
            b: false,
            a2: StructA {
                i: 3,
                x: 9003,
                s: "three".to_string(),
            },
            u: 222,
            a3: StructA {
                i: 4,
                x: 9004,
                s: "four".to_string(),
            },
        },
    };

    let handle_e4 = stash.stash(&e4);

    let mut e5 = EnumA::Four {
        a: StructA {
            i: 10,
            x: 90001,
            s: "one oh".to_string(),
        },
        b: StructB {
            a1: StructA {
                i: 20,
                x: 9002,
                s: "two oh".to_string(),
            },
            b: false,
            a2: StructA {
                i: 30,
                x: 9003,
                s: "three oh".to_string(),
            },
            u: 222,
            a3: StructA {
                i: 40,
                x: 9004,
                s: "four oh".to_string(),
            },
        },
    };

    assert_ne!(e4, e5);

    stash.unstash_inplace(&handle_e4, &mut e5).unwrap();

    assert_eq!(e4, e5);
}

#[derive(Copy, Clone)]
struct ExampleContextE {
    _n: usize,
}

#[derive(Eq, PartialEq, Debug, Stashable, Unstashable, UnstashableInplace)]
#[stashable_context(ExampleContextE)]
#[unstashable_context(ExampleContextE)]
#[unstashable_inplace_context(ExampleContextE)]
struct StructE {
    i: i32,
}

#[test]
fn test_derived_struct_e_with_context() {
    let stash = Stash::new();

    let e1 = StructE { i: 99 };

    let ctx = ExampleContextE { _n: 2 };

    let handle_e1 = stash.stash_with_context(&e1, ctx);

    let mut e2 = stash.unstash_with_context(&handle_e1, ctx).unwrap();

    assert_eq!(e2, e1);

    stash
        .unstash_inplace_with_context(&handle_e1, &mut e2, ctx)
        .unwrap();

    assert_eq!(e2, e1);
}

#[derive(Copy, Clone)]
struct ExampleContextF<'a> {
    _message: &'a str,
}

#[derive(Eq, PartialEq, Debug, Stashable, Unstashable, UnstashableInplace)]
#[stashable_context(for<'a> ExampleContextF<'a>)]
#[unstashable_context(for<'a> ExampleContextF<'a>)]
#[unstashable_inplace_context(for<'a> ExampleContextF<'a>)]
struct StructF {
    i: i32,
}

#[test]
fn test_derived_struct_f_with_generic_context() {
    let stash = Stash::new();

    let f1 = StructF { i: 100 };

    let ctx = ExampleContextF {
        _message: "better luck next time",
    };

    let handle_f1 = stash.stash_with_context(&f1, ctx);

    let mut f2 = stash.unstash_with_context(&handle_f1, ctx).unwrap();

    assert_eq!(f2, f1);

    stash
        .unstash_inplace_with_context(&handle_f1, &mut f2, ctx)
        .unwrap();

    assert_eq!(f2, f1);
}
