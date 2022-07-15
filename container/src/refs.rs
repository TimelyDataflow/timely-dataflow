/// Either an immutable or mutable reference.
pub enum RefOrMut<'a, T> {
    /// An immutable reference.
    Ref(&'a T),
    /// A mutable reference.
    Mut(&'a mut T),
    /// An owned type.
    Owned(T),
}

impl<T> ::std::ops::Deref for RefOrMut<'_, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        match self {
            RefOrMut::Ref(reference) => reference,
            RefOrMut::Mut(reference) => reference,
            RefOrMut::Owned(owned) => owned,
        }
    }
}

impl<T> ::std::borrow::Borrow<T> for RefOrMut<'_, T> {
    fn borrow(&self) -> &T {
        match self {
            RefOrMut::Ref(reference) => reference,
            RefOrMut::Mut(reference) => reference,
            RefOrMut::Owned(owned) => owned,
        }
    }
}

impl<T: Clone> RefOrMut<'_, T> {
    /// Extracts the contents of `self`, either by cloning or swapping.
    ///
    /// This consumes `self` because its contents are now in an unknown state.
    pub fn swap<'b>(self, element: &'b mut T) {
        match self {
            RefOrMut::Ref(reference) => element.clone_from(reference),
            RefOrMut::Mut(reference) => ::std::mem::swap(reference, element),
            RefOrMut::Owned(owned) => *element = owned,
        };
    }
    /// Extracts the contents of `self`, either by cloning or swapping.
    ///
    /// This consumes `self` because its contents are now in an unknown state.
    pub fn replace(self, mut element: T) -> T {
        self.swap(&mut element);
        element
    }

    /// Extracts the contents of `self`, either by cloning, or swapping and leaving a default
    /// element in place.
    ///
    /// This consumes `self` because its contents are now in an unknown state.
    pub fn take(self) -> T where T: Default {
        let mut element = Default::default();
        self.swap(&mut element);
        element
    }
}
