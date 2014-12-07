use std::default::Default;

pub trait CountMap<T> : Default
{
    fn update(&mut self, key: T, val: i64) -> i64;
    fn elements<'a>(&'a self) -> &'a Vec<(T, i64)>;
    fn clear(&mut self);
}

impl<T:Eq+Copy+'static> CountMap<T, > for Vec<(T, i64)>
{
    #[inline(always)]
    fn update(&mut self, key: T, val: i64) -> i64
    {
        let mut remove_at = None;
        let mut found = false;
        let mut new_val = val;

        for (index, &(k, ref mut v)) in self.iter_mut().enumerate()
        {
            if k.eq(&key)
            {
                found = true;
                *v += val;
                new_val = *v;

                if new_val == 0 { remove_at = Some(index); }
            }
        }

        if !found { self.push((key, val)); }
        if let Some(index) = remove_at { self.swap_remove(index); }
        return new_val;
    }

    fn elements<'a>(&'a self) -> &'a Vec<(T, i64)> { self }
    fn clear(&mut self) { self.clear(); }
}
