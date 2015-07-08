use std::fmt::Debug;
use std::default::Default;
use std::cmp::Ordering;

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
    occurrences:    CountMap<T>,    // occurrence count of each time
    precedents:     Vec<(T, i64)>,  // counts number of distinct times in occurences strictly less than element
    elements:       Vec<T>,         // the set of times with precedent count == 0
}

impl<T: PartialOrd+Eq+Clone+Debug+'static> MutableAntichain<T> {
    pub fn new() -> MutableAntichain<T> {
        MutableAntichain {
            occurrences:    Default::default(),
            precedents:     Default::default(),
            elements:       Vec::new(),
        }
    }

    pub fn elements(&self) -> &[T] { &self.elements }

    pub fn new_bottom(bottom: T) -> MutableAntichain<T> {
        let mut result = MutableAntichain::new();
        result.update_weight(&bottom, 1, &mut Default::default());
        return result;
    }

    pub fn empty(&self) -> bool { self.elements.len() == 0 }

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
        self.occurrences.elements().iter().filter(|x| time.eq(&x.0)).next().map(|i| i.1)
    }

    // TODO : Four different versions of basically the same code. Fix that!
    // TODO : Should this drain updates through to the CM? Check out uses!
    pub fn update_into_cm(&mut self, updates: &CountMap<T>, results: &mut CountMap<T>) -> () {
        self.update_iter_and(updates.elements().iter().map(|x| x.clone()), |time, val| { results.update(time, val); });
    }

    pub fn update_weight(&mut self, elem: &T, delta: i64, results: &mut CountMap<T>) -> () {
        self.update_and(elem, delta, |time, delta| { results.update(time, delta); });
    }

    #[inline] pub fn update(&mut self, elem: &T, delta: i64) { self.update_and(elem, delta, |_,_| {}); }

    //#[inline(always)]
    pub fn update_iter_and<I: Iterator<Item = (T, i64)>,
                           A: FnMut(&T, i64) -> ()>(&mut self, updates: I, mut action: A) -> () {
        for (ref elem, delta) in updates {
            self.update_and(elem, delta, |t,d| action(t,d));
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

            // if the value went from non-positive to positive we need to update self.precedents
            if old_value <= 0 && new_value > 0 {
                let mut preceded_by = 0;

                // maintain precedent counts relative to the set
                for &mut (ref key, ref mut val) in self.precedents.iter_mut() {
                    if let Some(comparison) = elem.partial_cmp(key) {
                        match comparison {
                            Ordering::Less    => {
                                if *val == 0 {
                                    self.elements.retain(|x| x != key);
                                    action(key, -1);
                                }
                                *val += 1;
                            },
                            Ordering::Equal   => { panic!("surprising!"); },
                            Ordering::Greater => { preceded_by += 1; },
                        }
                    }
                }

                // insert count always; maybe put in elements
                self.precedents.push((elem.clone(), preceded_by));
                if preceded_by == 0 {
                    self.elements.push(elem.clone());
                    action(elem, 1);
                }
            }

            // if the value went from positive to non-positive we need to update self.precedents.
            if old_value > 0 && new_value <= 0 {
                // maintain precedent counts relative to the set
                for &mut (ref key, ref mut val) in self.precedents.iter_mut() {
                    if elem < key {
                        *val -= 1;
                        if *val == 0 {
                            self.elements.push(key.clone());
                            action(key, 1);
                        }
                    }
                }

                if let Some(position) = self.elements.iter().position(|x| x == elem) {
                    action(elem, -1);
                    self.elements.swap_remove(position);
                }

                self.precedents.retain(|x| &x.0 != elem);
            }
        }
    }
}
