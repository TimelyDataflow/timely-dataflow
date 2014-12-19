use core::fmt::Show;
use std::default::Default;

use progress::count_map::CountMap;

#[deriving(Default, Clone, Show)]
pub struct Antichain<T>
{
    pub elements: Vec<T>
}

impl<T: PartialOrd+Eq+Copy+Show> Antichain<T>
{
    // returns true if element is added to the set
    pub fn insert(&mut self, element: T) -> bool
    {
        // bail if any element exceeds the candidate
        if !self.elements.iter().any(|x| element.ge(x))
        {
            let mut removed = 0;
            for index in range(0, self.elements.len())
            {
                let new_index = index - removed;
                if element.lt(&self.elements[new_index])
                {
                    self.elements.swap_remove(new_index);
                    removed += 1;
                }
            }

            self.elements.push(element);
            return true;
        }
        else
        {
            return false;
        }
    }

    pub fn new() -> Antichain<T> { Antichain { elements: Vec::new() } }

    pub fn from_elem(element: T) -> Antichain<T> { Antichain { elements: vec![element] } }
}

#[deriving(Default, Show)]
pub struct MutableAntichain<T:Eq>
{
    occurrences:    Vec<(T, i64)>,  // occurrence count of each time
    precedents:     Vec<(T, i64)>,  // precedent count of each time with occurrence count > 0
    pub elements:   Vec<T>,     // the set of times with precedent count == 0
}

impl<T: PartialOrd+Eq+Copy+Show+'static> MutableAntichain<T>
{
    pub fn new() -> MutableAntichain<T>
    {
        MutableAntichain
        {
            occurrences: Default::default(),
            precedents: Default::default(),
            elements: Vec::new(),
        }
    }

    pub fn new_bottom(bottom: T) -> MutableAntichain<T>
    {
        let mut result = MutableAntichain::new();

        result.update_weight(&bottom, 1, &mut Default::default());

        return result;
    }

    pub fn update_into_cm(&mut self, updates: &Vec<(T, i64)>, results: &mut Vec<(T, i64)>) -> ()
    {
        self.update_iter_and(updates.iter().map(|&x| x), |time, val| { results.update(time, val); });
    }

    pub fn update_weight(&mut self, elem: &T, delta: i64, results: &mut Vec<(T, i64)>) -> ()
    {
        let new_value = self.occurrences.update(*elem, delta);
        let old_value = new_value - delta;

        // introducing the time to the set
        if old_value <=0 && new_value > 0
        {
            let mut preceded_by = 0;

            // maintain precedent counts relative to the set
            for &(key, ref mut val) in self.precedents.iter_mut()
            {
                if key.gt(elem)
                {
                    if *val == 0
                    {
                        // find and remove key from elements
                        let mut found_index = 0;
                        for i in range(0, self.elements.len()) { if self.elements[i].eq(&key) { found_index = i; }}
                        self.elements.swap_remove(found_index);

                        results.update(key, -1);
                    }
                    *val += 1;
                }
                else
                {
                    preceded_by += 1;
                }
            }

            // insert count always; maybe put in elements
            self.precedents.push((*elem, preceded_by));
            if preceded_by == 0
            {
                self.elements.push(*elem);
                results.update(*elem, 1);
            }
        }

        // removing the time from the set
        if old_value > 0 && new_value <= 0
        {
            // maintain precedent counts relative to the set
            for &(key, ref mut val) in self.precedents.iter_mut()
            {
                if key.gt(elem)
                {
                    *val -= 1;
                    if *val == 0
                    {
                        self.elements.push(key);
                        results.update(key, 1);
                    }
                }
            }

            // remove elem if in elements
            if self.elements.contains(elem)
            {
                //self.elements.remove(elem);
                let mut found_index = 0;
                for i in range(0, self.elements.len()) { if self.elements[i].eq(elem) { found_index = i; }}
                self.elements.swap_remove(found_index);

                results.update(*elem, -1);
            }

            let mut to_remove = -1;
            for index in range(0, self.precedents.len())
            {
                let (ref key, _) = self.precedents[index];

                if key.eq(elem) { to_remove = index; }
            }

            self.precedents.swap_remove(to_remove);
        }
    }


    //#[inline(always)]
    pub fn update_iter_and<I: Iterator<(T, i64)>>(&mut self, mut updates: I, action: |T, i64| -> ()) -> ()
    {
        for (ref elem, delta) in updates
        {
            let new_value = self.occurrences.update(*elem, delta);
            let old_value = new_value - delta;

            // introducing the time to the set
            if old_value <= 0 && new_value > 0
            {
                let mut preceded_by = 0;

                // maintain precedent counts relative to the set
                for &(key, ref mut val) in self.precedents.iter_mut()
                {
                    if key.gt(elem)
                    {
                        if *val == 0
                        {
                            // find and remove key from elements
                            let mut found_index = 0;
                            for i in range(0, self.elements.len()) { if self.elements[i].eq(&key) { found_index = i; }}
                            self.elements.swap_remove(found_index);

                            action(key, -1);
                        }
                        *val += 1;
                    }
                    else
                    {
                        preceded_by += 1;
                    }
                }

                // insert count always; maybe put in elements
                self.precedents.push((*elem, preceded_by));
                if preceded_by == 0
                {
                    self.elements.push(*elem);
                    action(*elem, 1);
                }
            }

            // removing the time from the set
            if old_value > 0 && new_value <= 0
            {
                // maintain precedent counts relative to the set
                for &(key, ref mut val) in self.precedents.iter_mut()
                {
                    if key.gt(elem)
                    {
                        *val -= 1;
                        if *val == 0
                        {
                            self.elements.push(key);
                            action(key, 1);
                        }
                    }
                }

                // remove elem if in elements
                if self.elements.contains(elem)
                {
                    //self.elements.remove(elem);
                    let mut found_index = 0;
                    for i in range(0, self.elements.len()) { if self.elements[i].eq(elem) { found_index = i; }}
                    self.elements.swap_remove(found_index);

                    action(*elem, -1);
                }

                let mut to_remove = -1;
                for index in range(0, self.precedents.len())
                {
                    let (ref key, _) = self.precedents[index];

                    if key.eq(elem) { to_remove = index; }
                }

                self.precedents.swap_remove(to_remove);
            }
        }
    }

    #[inline]
    pub fn update_and(&mut self, elem: T, delta: i64, action: |T, i64| -> ()) -> ()
    {
        if delta != 0
        {
            if self.occurrences.len() > 100
            {
                println!("occurrences:\tmessed up (len() = {})", self.occurrences.len());
                // for &(key, val) in self.occurrences.iter()
                // {
                //     println!("\toccurrence: {} : {}", key, val);
                // }
            }
            if self.precedents.len() > 100 { println!("precedents:\tmessed up"); }

            let new_value = self.occurrences.update(elem, delta);
            let old_value = new_value - delta;

            // introducing the time to the set
            if old_value <= 0 && new_value > 0
            {
                let mut preceded_by = 0;

                // maintain precedent counts relative to the set
                for &(key, ref mut val) in self.precedents.iter_mut()
                {
                    if key.gt(&elem)
                    {
                        if *val == 0
                        {
                            // find and remove key from elements
                            let mut found_index = 0;
                            for i in range(0, self.elements.len()) { if self.elements[i].eq(&key) { found_index = i; }}
                            self.elements.swap_remove(found_index);

                            action(key, -1);
                        }
                        *val += 1;
                    }
                    else
                    {
                        preceded_by += 1;
                    }
                }

                // insert count always; maybe put in elements
                self.precedents.push((elem, preceded_by));
                if preceded_by == 0
                {
                    self.elements.push(elem);
                    action(elem, 1);
                }
            }

            // removing the time from the set
            if old_value > 0 && new_value <= 0
            {
                // maintain precedent counts relative to the set
                for &(key, ref mut val) in self.precedents.iter_mut()
                {
                    if key.gt(&elem)
                    {
                        *val -= 1;
                        if *val == 0
                        {
                            self.elements.push(key);
                            action(key, 1);
                        }
                    }
                }

                // remove elem if in elements
                if self.elements.contains(&elem)
                {
                    let mut found_index = 0;
                    for i in range(0, self.elements.len()) { if self.elements[i].eq(&elem) { found_index = i; }}
                    self.elements.swap_remove(found_index);

                    action(elem, -1);
                }

                let mut to_remove = -1;
                for index in range(0, self.precedents.len())
                {
                    let (ref key, _) = self.precedents[index];

                    if key.eq(&elem) { to_remove = index; }
                }

                self.precedents.swap_remove(to_remove);
            }
        }
    }
}
