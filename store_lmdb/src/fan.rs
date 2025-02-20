use rand::Rng;
use rsnano_core::RawKey;

/// The fan spreads a key out over the heap to decrease the likelihood of it being recovered by memory inspection
pub struct Fan {
    values: Vec<Box<RawKey>>,
}

impl Fan {
    pub fn new(key: RawKey, count: usize) -> Self {
        let mut first = Box::new(key);
        let mut values = Vec::with_capacity(count);
        let mut rng = rand::rng();
        for _ in 1..count {
            let entry = Box::new(RawKey::from_bytes(rng.random()));
            *first.as_mut() ^= entry.as_ref().clone();
            values.push(entry);
        }
        values.push(first);

        Self { values }
    }

    pub fn value(&self) -> RawKey {
        let mut key = RawKey::zero();
        for i in self.values.iter() {
            key ^= i.as_ref().clone();
        }
        key
    }

    pub fn value_set(&mut self, new_value: RawKey) {
        let old_value = self.value();
        *self.values[0] ^= old_value;
        *self.values[0] ^= new_value;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconstitute_fan() {
        let value0 = RawKey::from_bytes([0; 32]);
        let fan = Fan::new(value0, 1024);
        for i in fan.values.iter() {
            assert_ne!(i.as_ref(), &value0);
        }
        let value1 = fan.value();
        assert_eq!(value0, value1);
    }

    #[test]
    fn change_fan() {
        let value0 = RawKey::from_bytes([0; 32]);
        let value1 = RawKey::from_bytes([1; 32]);
        let mut fan = Fan::new(value0, 1024);
        assert_eq!(fan.values.len(), 1024);
        assert_eq!(fan.value(), value0);
        fan.value_set(value1);
        assert_eq!(fan.value(), value1);
    }
}
