//! Hash-consing with automated release of unused values

#[macro_use]
extern crate log;

use std::ops::{Deref, Drop};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::fmt::{self, Debug};
use std::marker::PhantomData;


/// Box that encapsulates a value to hash-cons, a reference to the conser,
/// and a reference counter
struct HashConsedBox<T>
    where T: Eq + Hash
{
    value: T,
    conser: HashConser<T>,
    refs: usize,
}

/// Unsafed reference to a hash-consed value
///
/// It is simply a pointer to the hash-consed box that encapsulate the raw value,
/// a reference to the conser and the current number of references.
///
/// **N.B.:** An unsafed hash-consed value:
///
///   * may or may not be in the conser's map,
///
///   * should be `destroy()`-ed manually,
///
///   * does not update automatically the ref count,
///
///   * inherits PartialEq, Eq, and Hash from the raw value.
struct UnsafeRef<T> where T: Eq + Hash {
    ptr: *mut HashConsedBox<T>,
    _marker: PhantomData<HashConsedBox<T>>,
}

impl<T> UnsafeRef<T> where T: Eq + Hash {

    /// Make an unsafed reference to a owned hash-consed box
    #[inline]
    fn make(conser: &HashConser<T>, value: T) -> Self {
        UnsafeRef {
            ptr: Box::into_raw(Box::new(HashConsedBox {
                value: value,
                conser: conser.clone(),
                refs: 0,
            })),
            _marker: PhantomData
        }
    }

    /// Destroy (drop) the underlying hash-consed box
    #[inline]
    fn destroy(&self) {
        drop(unsafe { Box::from_raw(self.ptr) });
    }

    /// Get pointer to conser
    #[inline]
    fn conser(&self) -> &mut HashConser<T> {
        unsafe { &mut (*self.ptr).conser }
    }

    #[inline]
    fn refs(&self) -> usize {
        unsafe { (*self.ptr).refs }
    }

    #[inline]
    fn inc_refs(&self) {
        unsafe { (*self.ptr).refs += 1; }
    }

    #[inline]
    fn dec_refs(&self) {
        unsafe { (*self.ptr).refs += 1; }
    }

    #[inline]
    fn value(&self) -> &T {
        unsafe { &(*self.ptr).value }
    }

}

/// Hash the underlying value
impl<T> Hash for UnsafeRef<T> where T: Eq + Hash {

    #[inline]
    fn hash<H>(&self, h: &mut H)
        where H: Hasher
    {
        self.value().hash(h);
    }

}

/// Compare the underlying values
impl<T> PartialEq<UnsafeRef<T>> for UnsafeRef<T> where T: Eq + Hash {

    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.value() == other.value()
    }

}

impl<T> Eq for UnsafeRef<T> where T: Eq + Hash {}

impl<T> Clone for UnsafeRef<T> where T: Eq + Hash {

    #[inline]
    fn clone(&self) -> Self {
        *self
    }

}

impl<T> Copy for UnsafeRef<T> where T: Eq + Hash {}

/// Reference to a hash-consed value.
///
/// Built through a `HashConser`, it points to a single copy of the raw value existing in the
/// `HashConser`.
///
/// Uses fast pointer equality and hash.
pub struct HashConsed<T>(UnsafeRef<T>) where T: Eq + Hash;

impl<T> HashConsed<T> where T: Eq + Hash {

    /// Wrap an unsafe reference
    fn from_unsafe(u: &UnsafeRef<T>) -> Self {
        u.inc_refs();
        debug!("new ref {:p} ({} ref total)", u.value(), u.refs());
        HashConsed(*u)
    }


    /// Get parent conser
    pub fn conser(this: &Self) -> &HashConser<T> {
        this.0.conser()
    }
}

/// Get reference to the raw value
impl<T> Deref for HashConsed<T> where T: Eq + Hash {

    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        return self.0.value();
    }

}

/// Fast hash (pointer-based)
///
/// Beware that it does not make any sense if the compared values where built through different
/// `HashConser`.
impl<T> Hash for HashConsed<T> where T: Eq + Hash {

    #[inline]
    fn hash<H>(&self, h: &mut H)
        where H: Hasher
    {
        self.0.ptr.hash(h);
    }

}

/// Fast comparison (pointer-based)
///
/// Beware that it does not make any sense if the compared values where built through different
/// `HashConser`
impl<T> PartialEq<HashConsed<T>> for HashConsed<T> where T: Eq + Hash {

    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.0.ptr == other.0.ptr
    }

}

impl<T> Eq for HashConsed<T> where T: Eq + Hash {}

impl<T> Drop for HashConsed<T> where T: Eq + Hash {

    fn drop(&mut self) {
        self.0.dec_refs();
        debug!("del ref {:p} ({} refs remaining)",
               self.0.value(),
               self.0.refs());
        if self.0.refs() == 0 {
            debug!("del val {:p}", self.0.value());
            self.0.conser().remove(&self.0);
            self.0.destroy();
        }
    }

}

impl<T> Debug for HashConsed<T> where T: Eq + Hash + Debug {

    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        Debug::fmt(self.0.value(), fmt)
    }

}

/// Get a new reference to this hash-consed value.
impl<T> Clone for HashConsed<T> where T: Eq + Hash {

    fn clone(&self) -> Self {
        self.0.inc_refs();
        debug!("new ref {:p} (clone, {} refs total)",
               self.0.value(),
               self.0.refs());
        HashConsed(self.0)
    }

}

type HM<T> where T: Eq + Hash = HashMap<UnsafeRef<T>, UnsafeRef<T>>;

struct HashConserBox<T> where T: Eq + Hash {
    map: HM<T>,
    refs: usize,
}

/// Hash-conser, i.e. hash-consed value factory and cache.
pub struct HashConser<T> where T: Eq + Hash {
    ptr: *mut HashConserBox<T>,
    _marker: PhantomData<HashConserBox<T>>,
}

impl<T> HashConser<T> where T: Eq + Hash {

    /// Create a hash-conser.
    pub fn new() -> Self {
        HashConser {
            ptr: Box::into_raw(Box::new(HashConserBox {
                map: HashMap::new(),
                refs: 1,
            })),
            _marker: PhantomData,
        }
    }

    #[inline]
    fn map(&self) -> &mut HM<T> {
        unsafe { &mut (*self.ptr).map }
    }

    #[inline]
    fn refs(&self) -> usize {
        unsafe { (*self.ptr).refs }
    }

    #[inline]
    fn inc_refs(&self) {
        unsafe {
            (*self.ptr).refs += 1;
        }
    }

    #[inline]
    fn dec_refs(&self) {
        unsafe {
            (*self.ptr).refs -= 1;
        }
    }

    /// Make a hash-consed value from an unwrapped value
    pub fn make(&mut self, obj: T) -> HashConsed<T> {
        debug!("h-cons  {:p} in {:p}", &obj, self);
        let input = UnsafeRef::make(self, obj);
        let safe = match self.map().get(&input) {
            Some(output) => {
                debug!("recycle {:p} (already {} refs)",
                       output.value(),
                       output.refs());
                input.destroy();
                HashConsed::from_unsafe(output)
            }
            None => {
                debug!("new val {:p} in {:p}", input.value(), self);
                self.map().insert(input.clone(), input);
                HashConsed::from_unsafe(&input)
            }
        };
        debug!("/h-cons");
        safe
    }

    #[inline]
    fn remove(&mut self, hc: &UnsafeRef<T>) {
        self.map().remove(hc);
    }

}

impl<T> Clone for HashConser<T> where T: Eq + Hash {

    #[inline]
    fn clone(&self) -> Self {
        self.inc_refs();
        HashConser {
            ptr: self.ptr,
            _marker: PhantomData,
        }
    }

}

impl<T> Drop for HashConser<T> where T: Eq + Hash {

    fn drop(&mut self) {
        self.dec_refs();
        debug!("del ref HashConser({:p}) ({} refs remaining)",
               self.ptr,
               self.refs());
        if self.refs() == 0 {
            assert!(self.map().len() == 0);
            debug!("del val HashConser({:p})", self.ptr);
            let b = unsafe { Box::from_raw(self.ptr) };
            drop(b);
        }
    }

}

impl<T> Debug for HashConser<T> where T: Eq + Hash + Debug {

    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        try!(fmt.write_str("{"));
        for (i, k) in self.map().keys().enumerate() {
            if i != 0 {
                try!(fmt.write_str(", "));
            }
            try!(write!(fmt, "{:?} ({})", k.value(), k.refs()));
        }
        fmt.write_str("}")
    }

}


#[cfg(test)]
mod test {
    use super::*;

    #[derive(Debug,PartialEq,Eq,Hash)]
    struct Pair(u8, u8);
    type HCPair = HashConsed<Pair>;

    #[test]
    fn test_equality() {
        let mut conser = HashConser::new();
        let a: HCPair = conser.make(Pair(0,1));
        let b: HCPair = conser.make(Pair(0,1));
        assert_eq!(a, b);
        assert_eq!(&*a as *const Pair, &*b as *const Pair);
    }

    #[test]
    fn test_drop_conser() {
        let mut conser = HashConser::new();
        let a: HCPair = conser.make(Pair(0,1));
        let b: HCPair = conser.make(Pair(0,1));
        drop(conser);
        assert_eq!(a, b);
        assert_eq!(&*a as *const Pair, &*b as *const Pair);
    }
}
