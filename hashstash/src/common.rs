use std::{cell::RefCell, collections::HashMap, hash::Hash};

use crate::{
    InplaceUnstasher, Order, Stashable, Stasher, UnstashError, Unstashable, UnstashableInplace,
    Unstasher,
};

macro_rules! impl_traits_for_prim {
    ($primname:ident, $primname_inplace:ident) => {
        impl<C: Copy> Stashable<C> for $primname {
            fn stash(&self, stasher: &mut Stasher<C>) {
                stasher.$primname(*self);
            }
        }

        impl<C: Copy> Unstashable<C> for $primname {
            fn unstash(unstasher: &mut Unstasher<C>) -> Result<Self, UnstashError> {
                unstasher.$primname()
            }
        }

        impl<C: Copy> UnstashableInplace<C> for $primname {
            fn unstash_inplace(
                &mut self,
                unstasher: &mut InplaceUnstasher<C>,
            ) -> Result<(), UnstashError> {
                unstasher.$primname_inplace(self)
            }
        }
    };
}

impl_traits_for_prim!(bool, bool_inplace);
impl_traits_for_prim!(u8, u8_inplace);
impl_traits_for_prim!(i8, i8_inplace);
impl_traits_for_prim!(u16, u16_inplace);
impl_traits_for_prim!(i16, i16_inplace);
impl_traits_for_prim!(u32, u32_inplace);
impl_traits_for_prim!(i32, i32_inplace);
impl_traits_for_prim!(u64, u64_inplace);
impl_traits_for_prim!(i64, i64_inplace);
impl_traits_for_prim!(f32, f32_inplace);
impl_traits_for_prim!(f64, f64_inplace);
impl_traits_for_prim!(usize, usize_inplace);
impl_traits_for_prim!(isize, isize_inplace);

impl<C: Copy> Stashable<C> for String {
    fn stash(&self, stasher: &mut Stasher<C>) {
        stasher.string(self);
    }
}

impl<C: Copy> Unstashable<C> for String {
    fn unstash(unstasher: &mut Unstasher<C>) -> Result<Self, UnstashError> {
        unstasher.string()
    }
}

impl<C: Copy> UnstashableInplace<C> for String {
    fn unstash_inplace(&mut self, unstasher: &mut InplaceUnstasher<C>) -> Result<(), UnstashError> {
        unstasher.string_inplace(self)
    }
}

impl<C, T> Stashable<C> for Option<T>
where
    C: Copy,
    T: Stashable<C>,
{
    fn stash(&self, stasher: &mut Stasher<C>) {
        match self {
            None => stasher.u8(0),
            Some(x) => {
                stasher.u8(1);
                stasher.object(x);
            }
        }
    }
}

impl<C, T> Unstashable<C> for Option<T>
where
    C: Copy,
    T: 'static + Unstashable<C>,
{
    fn unstash(unstasher: &mut Unstasher<C>) -> Result<Self, UnstashError> {
        match unstasher.u8()? {
            0 => Ok(None),
            1 => Ok(Some(unstasher.object()?)),
            _ => Err(UnstashError::BadValue),
        }
    }
}

impl<C, T> UnstashableInplace<C> for Option<T>
where
    C: Copy,
    T: 'static + Unstashable<C> + UnstashableInplace<C>,
{
    fn unstash_inplace(&mut self, unstasher: &mut InplaceUnstasher<C>) -> Result<(), UnstashError> {
        match unstasher.u8()? {
            0 => match self {
                Some(_) => {
                    if unstasher.time_to_write() {
                        *self = None;
                    }
                    Ok(())
                }
                None => Ok(()),
            },
            1 => match self {
                Some(x) => unstasher.object_inplace(x),
                None => {
                    let x = unstasher.object()?;
                    if unstasher.time_to_write() {
                        *self = Some(x);
                    }
                    Ok(())
                }
            },
            _ => Err(UnstashError::BadValue),
        }
    }
}

impl<C, T> Stashable<C> for Box<T>
where
    C: Copy,
    T: Stashable<C>,
{
    fn stash(&self, stasher: &mut Stasher<C>) {
        self.as_ref().stash(stasher);
    }
}

impl<C, T> Unstashable<C> for Box<T>
where
    C: Copy,
    T: Unstashable<C>,
{
    fn unstash(unstasher: &mut Unstasher<C>) -> Result<Self, UnstashError> {
        Ok(Box::new(T::unstash(unstasher)?))
    }
}

impl<C, T> UnstashableInplace<C> for Box<T>
where
    C: Copy,
    T: UnstashableInplace<C>,
{
    fn unstash_inplace(&mut self, unstasher: &mut InplaceUnstasher<C>) -> Result<(), UnstashError> {
        self.as_mut().unstash_inplace(unstasher)?;
        Ok(())
    }
}

impl<C, T> Stashable<C> for RefCell<T>
where
    C: Copy,
    T: Stashable<C>,
{
    fn stash(&self, stasher: &mut Stasher<C>) {
        self.borrow().stash(stasher);
    }
}

impl<C, T> Unstashable<C> for RefCell<T>
where
    C: Copy,
    T: Unstashable<C>,
{
    fn unstash(unstasher: &mut Unstasher<C>) -> Result<Self, UnstashError> {
        Ok(RefCell::new(T::unstash(unstasher)?))
    }
}

impl<C, T> UnstashableInplace<C> for RefCell<T>
where
    C: Copy,
    T: UnstashableInplace<C>,
{
    fn unstash_inplace(&mut self, unstasher: &mut InplaceUnstasher<C>) -> Result<(), UnstashError> {
        self.get_mut().unstash_inplace(unstasher)?;
        Ok(())
    }
}

impl<C, K, V> Stashable<C> for HashMap<K, V>
where
    C: Copy,
    K: Stashable<C>,
    V: Stashable<C>,
{
    fn stash(&self, stasher: &mut Stasher<C>) {
        stasher.array_of_proxy_objects(
            self.iter(),
            |(k, v), stasher| {
                k.stash(stasher);
                v.stash(stasher);
            },
            Order::Unordered,
        );
    }
}

impl<C, K, V> Unstashable<C> for HashMap<K, V>
where
    C: Copy,
    K: Unstashable<C> + Hash + Eq,
    V: Unstashable<C>,
{
    fn unstash(unstasher: &mut Unstasher<C>) -> Result<Self, UnstashError> {
        let mut hm = HashMap::new();
        unstasher.array_of_proxy_objects(|unstasher| {
            hm.insert(K::unstash(unstasher)?, V::unstash(unstasher)?);
            Ok(())
        })?;
        Ok(hm)
    }
}

impl<C, K, V> UnstashableInplace<C> for HashMap<K, V>
where
    C: Copy,
    K: Unstashable<C> + Hash + Eq + Clone,
    V: Unstashable<C> + UnstashableInplace<C>,
{
    fn unstash_inplace(&mut self, unstasher: &mut InplaceUnstasher<C>) -> Result<(), UnstashError> {
        let mut unstashed_keys = Vec::<K>::new();

        let time_to_write = unstasher.time_to_write();

        unstasher.array_of_proxy_objects(|unstasher| {
            let k = K::unstash(unstasher)?;

            if time_to_write {
                unstashed_keys.push(k.clone());

                match self.get_mut(&k) {
                    Some(v) => {
                        unstasher.with_inplace_unstasher(time_to_write, |unstasher| {
                            v.unstash_inplace(unstasher)
                        })?;
                    }
                    None => {
                        let v = V::unstash(unstasher)?;
                        self.insert(k, v);
                    }
                }
            } else {
                let _ = V::unstash(unstasher)?;
            }

            Ok(())
        })?;

        if time_to_write {
            self.retain(|k, _| unstashed_keys.contains(k));
        }

        Ok(())
    }
}
