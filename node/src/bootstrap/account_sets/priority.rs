use ordered_float::OrderedFloat;
use std::ops::{Add, Deref, Div, Mul, Sub};

#[derive(PartialEq, Eq, Default, Clone, Copy, Ord, PartialOrd)]
pub(crate) struct Priority(OrderedFloat<f64>);

impl Priority {
    pub const fn new(value: f64) -> Self {
        Self(OrderedFloat(value))
    }

    pub const ZERO: Self = Self(OrderedFloat(0.0));

    pub fn as_f64(&self) -> f64 {
        self.0 .0
    }
}

impl Add for Priority {
    type Output = Priority;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

impl Sub for Priority {
    type Output = Priority;

    fn sub(self, rhs: Self) -> Self::Output {
        Self(self.0 - rhs.0)
    }
}

impl Mul<f64> for Priority {
    type Output = Priority;

    fn mul(self, rhs: f64) -> Self::Output {
        Self(self.0 * rhs)
    }
}

impl Div<f64> for Priority {
    type Output = Priority;

    fn div(self, rhs: f64) -> Self::Output {
        Self(self.0 / rhs)
    }
}

impl From<Priority> for f64 {
    fn from(value: Priority) -> Self {
        value.as_f64()
    }
}

impl std::fmt::Debug for Priority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0 .0, f)
    }
}

impl std::fmt::Display for Priority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0 .0, f)
    }
}

/// Priority in descending order
#[derive(PartialEq, Eq, Default, Clone, Copy)]
pub(crate) struct PriorityKeyDesc(pub Priority);

impl Ord for PriorityKeyDesc {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // order descending
        other.0.cmp(&self.0)
    }
}

impl PartialOrd for PriorityKeyDesc {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Deref for PriorityKeyDesc {
    type Target = Priority;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Priority> for PriorityKeyDesc {
    fn from(value: Priority) -> Self {
        Self(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create() {
        assert_priority_eq(Priority::new(1.0), Priority::new(1.0));
        assert!(Priority::new(1.0) != Priority::new(1.1));
        assert_eq!(Priority::new(1.23).as_f64(), 1.23);
    }

    #[test]
    fn add() {
        assert_priority_eq(Priority::new(1.0) + Priority::new(2.5), Priority::new(3.5));
    }

    #[test]
    fn sub() {
        assert_priority_eq(Priority::new(2.4) - Priority::new(1.1), Priority::new(1.3));
    }

    #[test]
    fn mul() {
        assert_priority_eq(Priority::new(2.4) * 2.0, Priority::new(4.8));
    }

    #[test]
    fn div() {
        assert_priority_eq(Priority::new(2.4) / 2.0, Priority::new(1.2));
    }

    #[test]
    fn format() {
        assert_eq!(format!("{}", Priority::new(1.23)), "1.23");
        assert_eq!(format!("{:?}", Priority::new(1.23)), "1.23");
    }

    fn assert_priority_eq(actual: Priority, expected: Priority) {
        let actual = actual.as_f64();
        let expected = expected.as_f64();
        let diff = actual - expected;
        assert!(
            diff.abs() < 0.001,
            "expected priority {} to be equal to {}",
            actual,
            expected
        );
    }
}
