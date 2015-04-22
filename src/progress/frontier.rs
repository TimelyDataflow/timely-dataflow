use std::fmt::Debug;
use std::default::Default;

use progress::CountMap;

#[derive(Default, Clone, Debug)]
pub struct Antichain<T> {
    pub elements: Vec<T>
}

impl<T: PartialOrd+Eq+Copy+Debug> Antichain<T> {
    // returns true if element is added to the set
    pub fn insert(&mut self, element: T) -> bool {
        // bail if any element exceeds the candidate
        if !self.elements.iter().any(|x| element.ge(x)) {
            let mut removed = 0;
            for index in (0..self.elements.len()) {
                let new_index = index - removed;
                if element.lt(&self.elements[new_index]) {
                    self.elements.swap_remove(new_index);
                    removed += 1;
                }
            }

            self.elements.push(element);
            return true;
        }
        else {
            return false;
        }
    }

    pub fn new() -> Antichain<T> { Antichain { elements: Vec::new() } }
    pub fn from_elem(element: T) -> Antichain<T> { Antichain { elements: vec![element] } }
}

#[derive(Default, Debug, Clone)]
pub struct MutableAntichain<T:Eq> {
    pub occurrences:    CountMap<T>,    // occurrence count of each time
    precedents:         Vec<(T, i64)>,    // precedent count of each time with occurrence count > 0
    pub elements:       Vec<T>,         // the set of times with precedent count == 0
}

impl<T: PartialOrd+Eq+Clone+Debug+'static> MutableAntichain<T> {
    pub fn new() -> MutableAntichain<T> {
        MutableAntichain {
            occurrences:    Default::default(),
            precedents:     Default::default(),
            elements:       Vec::new(),
        }
    }

    pub fn new_bottom(bottom: T) -> MutableAntichain<T> {
        let mut result = MutableAntichain::new();
        result.update_weight(&bottom, 1, &mut Default::default());
        return result;
    }

    #[inline]
    pub fn lt(&self, time: &T) -> bool {
        self.elements.iter().any(|x| x.lt(time))
    }

    #[inline]
    pub fn le(&self, time: &T) -> bool {
        self.elements.iter().any(|x| x.le(time))
    }

    #[inline]
    pub fn count(&self, time: &T) -> Option<i64> {
        for &(ref key, val) in self.occurrences.elements().iter() {
            if time.eq(key) { return Some(val); }
        }
        return None;
    }

    // TODO : Four different versions of basically the same code. Fix that!
    // TODO : Should this drain updates through to the CM? Check out uses!
    pub fn update_into_cm(&mut self, updates: &CountMap<T>, results: &mut CountMap<T>) -> () {
        self.update_iter_and(updates.elements().iter().map(|x| x.clone()), |time, val| { results.update(time, val); });
    }

    pub fn update_weight(&mut self, elem: &T, delta: i64, results: &mut CountMap<T>) -> () {
        self.update_and(elem, delta, |time, delta| { results.update(time, delta); });
    }


    //#[inline(always)]
    pub fn update_iter_and<I: Iterator<Item = (T, i64)>, A: FnMut(&T, i64) -> ()>(&mut self, updates: I, mut action: A) -> () {
        for (ref elem, delta) in updates {
            // self.update_and(elem, delta, action);
            let new_value = self.occurrences.update(elem, delta);
            let old_value = new_value - delta;

            // introducing the time to the set
            if old_value <= 0 && new_value > 0 {
                let mut preceded_by = 0;

                // maintain precedent counts relative to the set
                for &mut (ref key, ref mut val) in self.precedents.iter_mut() {
                    if key.gt(elem) {
                        if *val == 0 {
                            // find and remove key from elements
                            let mut found_index = 0;
                            for i in (0..self.elements.len()) { if self.elements[i].eq(key) { found_index = i; }}
                            self.elements.swap_remove(found_index);

                            action(key, -1);
                        }
                        *val += 1;
                    }
                    else {
                        preceded_by += 1;
                    }
                }

                // insert count always; maybe put in elements
                self.precedents.push((elem.clone(), preceded_by));
                if preceded_by == 0 {
                    self.elements.push(elem.clone());
                    action(elem, 1);
                }
            }

            // removing the time from the set
            if old_value > 0 && new_value <= 0 {
                // maintain precedent counts relative to the set
                for &mut (ref key, ref mut val) in self.precedents.iter_mut() {
                    if key.gt(elem) {
                        *val -= 1;
                        if *val == 0 {
                            self.elements.push(key.clone());
                            action(key, 1);
                        }
                    }
                }

                // remove elem if in elements
                if self.elements.contains(elem) {
                    //self.elements.remove(elem);
                    let mut found_index = 0;
                    for i in (0..self.elements.len()) { if self.elements[i].eq(elem) { found_index = i; }}
                    self.elements.swap_remove(found_index);

                    action(elem, -1);
                }

                let mut to_remove = -1;
                for index in (0..self.precedents.len()) {
                    if self.precedents[index].0.eq(elem) { to_remove = index; }
                }

                self.precedents.swap_remove(to_remove);
            }
        }
    }

    #[inline]
    pub fn test_size(&self, threshold: usize, name: &str) {
        if self.occurrences.len() > threshold {
            println!("{}:\toccurrences:\tlen() = {}", name, self.occurrences.len());
            // for &(ref key, val) in self.occurrences.elements().iter() { println!("{}: \toccurrence: {:?} : {:?}", name, key, val); }
            // panic!();
        }
        if self.precedents.len() > threshold {
            println!("{}: precedents:\tlen() = {}", name, self.precedents.len());
        }
    }

    #[inline]
    pub fn update_and<A: FnMut(&T, i64)->()>(&mut self, elem: &T, delta: i64, mut action: A) -> () {
        if delta != 0 {
            // self.test_size(100, "???");
            let new_value = self.occurrences.update(elem, delta);
            let old_value = new_value - delta;

            // introducing the time to the set
            if old_value <= 0 && new_value > 0 {
                let mut preceded_by = 0;

                // maintain precedent counts relative to the set
                for &mut (ref key, ref mut val) in self.precedents.iter_mut() {
                    if key.gt(elem) {
                        if *val == 0 {
                            // find and remove key from elements
                            let mut found_index = 0;
                            for i in (0..self.elements.len()) { if self.elements[i].eq(key) { found_index = i; }}
                            self.elements.swap_remove(found_index);

                            action(key, -1);
                        }
                        *val += 1;
                    }
                    else {
                        preceded_by += 1;
                    }
                }

                // insert count always; maybe put in elements
                self.precedents.push((elem.clone(), preceded_by));
                if preceded_by == 0 {
                    self.elements.push(elem.clone());
                    action(elem, 1);
                }
            }

            // removing the time from the set
            if old_value > 0 && new_value <= 0 {
                // maintain precedent counts relative to the set
                for &mut (ref key, ref mut val) in self.precedents.iter_mut() {
                    if key.gt(elem) {
                        *val -= 1;
                        if *val == 0 {
                            self.elements.push(key.clone());
                            action(key, 1);
                        }
                    }
                }

                // remove elem if in elements
                if self.elements.contains(elem) {
                    let mut found_index = 0;
                    for i in (0..self.elements.len()) { if self.elements[i].eq(elem) { found_index = i; }}
                    self.elements.swap_remove(found_index);

                    action(elem, -1);
                }

                let mut to_remove = -1;
                for index in (0..self.precedents.len()) {
                    if self.precedents[index].0.eq(elem) { to_remove = index; }
                }

                self.precedents.swap_remove(to_remove);
            }
        }
    }

    #[inline] pub fn update(&mut self, elem: &T, delta: i64) { self.update_and(elem, delta, |_,_| {}); }
}
