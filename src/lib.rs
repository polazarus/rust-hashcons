//! Hash-consing with automated release of unused values

#[macro_use]
extern crate log;

use std::ops::{Deref, Drop};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::fmt::{self, Debug};
use std::cell::UnsafeCell;


/// Box that encapsulates a value to hash-cons, a reference to the conser, and a reference counter.
struct HashConsedBox<T>
    where T: Hash + Eq
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
struct UnsafeRef<T>(*mut UnsafeCell<HashConsedBox<T>>) where T: Hash + Eq;

impl<T> UnsafeRef<T> where T: Hash + Eq {

    /// Make an unsafed reference to a owned hash-consed box
    #[inline]
    fn make(conser: &HashConser<T>, value: T) -> Self {
        UnsafeRef(Box::into_raw(Box::new(UnsafeCell::new(HashConsedBox {
            value: value,
            conser: conser.copy(),
            refs: 0,
        }))))
    }

    /// Destroy (drop) the underlying hash-consed box
    #[inline]
    fn destroy(&self) {
        drop(unsafe { Box::from_raw(self.0) });
    }

    /// Get pointer to conser
    #[inline]
    fn conser(&mut self) -> *mut HashConser<T> {
        unsafe { &mut (*self.as_ptr()).conser as *mut HashConser<T> }
    }

    #[inline]
    fn refs(&self) -> usize {
        unsafe { (*self.as_ptr()).refs }
    }

    #[inline]
    fn inc_refs(&self) {
        let refs = unsafe { &mut (*self.as_ptr()).refs };
        *refs += 1;
    }

    #[inline]
    fn dec_refs(&self) {
        let refs = unsafe { &mut (*self.as_ptr()).refs };
        *refs -= 1;
    }

    #[inline]
    fn value(&self) -> &T {
        unsafe { &(*self.as_ptr()).value }
    }

    #[inline]
    fn as_ptr(&self) -> *mut HashConsedBox<T> {
        unsafe { (*self.0).get() }
    }
}

/// Hash the underlying value
impl<T> Hash for UnsafeRef<T> where T: Hash + Eq {

    #[inline]
    fn hash<H>(&self, h: &mut H)
        where H: Hasher
    {
        self.value().hash(h);
    }

}

/// Compare the underlying values
impl<T> PartialEq<UnsafeRef<T>> for UnsafeRef<T> where T: Hash + Eq {

    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.value() == other.value()
    }

}

impl<T> Eq for UnsafeRef<T> where T: Hash + Eq {
}

/// Clone a new reference to the hash-consed value
impl<T> Clone for UnsafeRef<T> where T: Hash + Eq {

    #[inline]
    fn clone(&self) -> Self {
        UnsafeRef(self.0)
    }

}

impl<T> Copy for UnsafeRef<T> where T: Hash + Eq {}

/// Reference to a hash-consed value.
///
/// Built through a `HashConser`, it points to a single copy of the raw value existing in the
/// `HashConser`.
///
/// Uses fast pointer equality and hash.
pub struct HashConsed<T>(UnsafeRef<T>) where T: Hash + Eq;

impl<T> HashConsed<T> where T: Hash + Eq {
    /// Wrap an unsafe reference
    fn from_unsafe(u: &UnsafeRef<T>) -> Self {
        u.inc_refs();
        debug!("new ref {:p} ({} ref total)", u.value(), u.refs());
        HashConsed(*u)
    }

    /// Get underlying value
    #[inline]
    pub fn value(&self) -> &T {
        self.0.value()
    }

    /// Get number of references
    #[cfg(test)]
    pub fn refs(&self) -> usize {
        self.0.refs()
    }
}

/// Get reference to the raw value
impl<T> Deref for HashConsed<T> where T: Hash + Eq {
  type Target = T;
    fn deref(&self) -> &T {
        return self.value();
    }
}

/// Fast hash (pointer-based)
///
/// Beware that it does not make any sense if the compared values where built through different
/// `HashConser`.
impl<T> Hash for HashConsed<T> where T: Hash + Eq {
    fn hash<H>(&self, h: &mut H)
        where H: Hasher
    {
        self.0.as_ptr().hash(h);
    }
}

/// Fast comparison (pointer-based)
///
/// Beware that it does not make any sense if the compared values where built through different
/// `HashConser`
impl<T> PartialEq<HashConsed<T>> for HashConsed<T> where T: Hash + Eq {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.0.as_ptr() == other.0.as_ptr()
    }
}

impl<T> Eq for HashConsed<T> where T: Hash + Eq {}

impl<T> Drop for HashConsed<T> where T: Hash + Eq {
    fn drop(&mut self) {
        if self.0.refs() == 0 {
            panic!();
        } else {
            self.0.dec_refs();
            debug!("del ref {:p} ({} refs remaining)",
                   self.0.value(),
                   self.0.refs());
            if self.0.refs() == 0 {
                debug!("del val {:p}", self.value());
                let conser = unsafe { &mut *self.0.conser() };
                conser.remove(&self.0);
                self.0.destroy();
            }
        }
    }
}

impl<T> Debug for HashConsed<T> where T: Hash + Eq + Debug {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        Debug::fmt(self.0.value(), fmt)
    }
}

/// Get a new reference to this hash-consed value.
impl<T> Clone for HashConsed<T> where T: Hash + Eq {
    fn clone(&self) -> Self {
        self.0.inc_refs();
        debug!("new ref {:p} (clone, {} refs total)",
               self.0.value(),
               self.0.refs());
        HashConsed(self.0)
    }
}

type HM<T> where T: Hash + Eq = HashMap<UnsafeRef<T>, UnsafeRef<T>>;

/// Hash-conser, i.e. hash-consed value factory and cache.
pub struct HashConser<T>(*mut UnsafeCell<HM<T>>) where T: Hash + Eq;

impl<T> HashConser<T> where T: Hash + Eq {

    /// Create a hash-conser.
    pub fn new() -> Self {
        HashConser(Box::into_raw(Box::new(UnsafeCell::new(HashMap::new()))))
    }

    #[inline]
    fn map(&self) -> &mut HM<T> {
        unsafe { &mut (*(*self.0).get()) }
    }

    /// Make a hash-consed value from an unwrapped value
    pub fn make(&mut self, obj: T) -> HashConsed<T> {
        debug!("h-cons  {:p} in {:p}", &obj, self);
        let input = UnsafeRef::make(self, obj);
        let map = self.map();
        let safe = match map.get(&input) {
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

    /// Private: get a new reference of the conference
    ///
    /// WARNING! no reference counting
    ///
    /// *Invariant:* the number of references is the number of hash-consed values plus one if the
    /// user stills holds the ref
    #[inline]
    fn copy(&self) -> Self {
        HashConser(self.0)
    }
}

impl<T> Drop for HashConser<T> where T: Hash + Eq {
    fn drop(&mut self) {
        debug!("del ref HashConser({:p}) ({} refs remaining)",
               self.0,
               self.map().len());
        if self.map().len() == 0 {
            debug!("del val HashConser({:p})", self.0);
            let b = unsafe { Box::from_raw(self.0) };
            drop(b);
        }
    }
}

impl<T> Debug for HashConser<T> where T: Hash + Eq + Debug {
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

#[test]
fn it_works() {}