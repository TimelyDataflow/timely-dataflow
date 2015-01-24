use std::default::Default;

pub trait CountMap<T> : Default
{
    fn update(&mut self, key: &T, val: i64) -> i64;
    fn elements<'a>(&'a self) -> &'a Vec<(T, i64)>;
    fn clear(&mut self);
}

impl<T:Eq+Clone+'static> CountMap<T, > for Vec<(T, i64)>
{
    #[inline(always)]
    fn update(&mut self, key: &T, val: i64) -> i64 {
        if self.len() > 100 { println!("perf: self.len() = {}", self.len()); }
        let mut remove_at = None;
        let mut found = false;
        let mut new_val = val;

        for (index, &mut (ref k, ref mut v)) in self.iter_mut().enumerate() {
            if k.eq(key) {
                found = true;
                *v += val;
                new_val = *v;

                if new_val == 0 { remove_at = Some(index); }
            }
        }

        if !found && val != 0 { self.push((key.clone(), val)); }
        if let Some(index) = remove_at { self.swap_remove(index); }
        return new_val;
    }

    fn elements<'a>(&'a self) -> &'a Vec<(T, i64)> { self }
    fn clear(&mut self) { self.clear(); }
}
