use std::{
    error::Error,
    fmt,
    ops::{
        Bound, Range as StdRange, RangeFrom, RangeFull, RangeInclusive, RangeTo, RangeToInclusive,
    },
};

use bytes::BytesMut;
use postgres_protocol::types::RangeBound;

use crate::{FromSql, IsNull, Kind, ToSql, Type};

/// A Postgres range.
#[derive(Debug)]
pub enum Range<T> {
    /// An empty range.
    Empty,
    /// A nonempty range.
    Nonempty {
        /// The lower bound.
        lower: Bound<T>,
        /// The upper bound.
        upper: Bound<T>,
    },
}

/// An error indicating a [`Range`] cannot be represented by a std range type.
#[derive(Debug, Clone, Copy)]
pub struct RangeConversionError {
    target: &'static str,
}

impl RangeConversionError {
    fn new(target: &'static str) -> Self {
        Self { target }
    }
}

impl fmt::Display for RangeConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "range cannot be represented as std::ops::{}",
            self.target
        )
    }
}

impl Error for RangeConversionError {}

impl<T> From<StdRange<T>> for Range<T> {
    fn from(value: StdRange<T>) -> Self {
        Range::Nonempty {
            lower: Bound::Included(value.start),
            upper: Bound::Excluded(value.end),
        }
    }
}

impl<T> From<RangeInclusive<T>> for Range<T> {
    fn from(value: RangeInclusive<T>) -> Self {
        let (lower, upper) = value.into_inner();
        Range::Nonempty {
            lower: Bound::Included(lower),
            upper: Bound::Included(upper),
        }
    }
}

impl<T> From<RangeFrom<T>> for Range<T> {
    fn from(value: RangeFrom<T>) -> Self {
        Range::Nonempty {
            lower: Bound::Included(value.start),
            upper: Bound::Unbounded,
        }
    }
}

impl<T> From<RangeTo<T>> for Range<T> {
    fn from(value: RangeTo<T>) -> Self {
        Range::Nonempty {
            lower: Bound::Unbounded,
            upper: Bound::Excluded(value.end),
        }
    }
}

impl<T> From<RangeToInclusive<T>> for Range<T> {
    fn from(value: RangeToInclusive<T>) -> Self {
        Range::Nonempty {
            lower: Bound::Unbounded,
            upper: Bound::Included(value.end),
        }
    }
}

impl<T> From<RangeFull> for Range<T> {
    fn from(_: RangeFull) -> Self {
        Range::Nonempty {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        }
    }
}

impl<T> TryFrom<Range<T>> for StdRange<T> {
    type Error = RangeConversionError;

    fn try_from(value: Range<T>) -> Result<Self, Self::Error> {
        match value {
            Range::Nonempty {
                lower: Bound::Included(start),
                upper: Bound::Excluded(end),
            } => Ok(start..end),
            _ => Err(RangeConversionError::new("Range")),
        }
    }
}

impl<T> TryFrom<Range<T>> for RangeInclusive<T> {
    type Error = RangeConversionError;

    fn try_from(value: Range<T>) -> Result<Self, Self::Error> {
        match value {
            Range::Nonempty {
                lower: Bound::Included(start),
                upper: Bound::Included(end),
            } => Ok(start..=end),
            _ => Err(RangeConversionError::new("RangeInclusive")),
        }
    }
}

impl<T> TryFrom<Range<T>> for RangeFrom<T> {
    type Error = RangeConversionError;

    fn try_from(value: Range<T>) -> Result<Self, Self::Error> {
        match value {
            Range::Nonempty {
                lower: Bound::Included(start),
                upper: Bound::Unbounded,
            } => Ok(start..),
            _ => Err(RangeConversionError::new("RangeFrom")),
        }
    }
}

impl<T> TryFrom<Range<T>> for RangeTo<T> {
    type Error = RangeConversionError;

    fn try_from(value: Range<T>) -> Result<Self, Self::Error> {
        match value {
            Range::Nonempty {
                lower: Bound::Unbounded,
                upper: Bound::Excluded(end),
            } => Ok(..end),
            _ => Err(RangeConversionError::new("RangeTo")),
        }
    }
}

impl<T> TryFrom<Range<T>> for RangeToInclusive<T> {
    type Error = RangeConversionError;

    fn try_from(value: Range<T>) -> Result<Self, Self::Error> {
        match value {
            Range::Nonempty {
                lower: Bound::Unbounded,
                upper: Bound::Included(end),
            } => Ok(..=end),
            _ => Err(RangeConversionError::new("RangeToInclusive")),
        }
    }
}

impl<T> TryFrom<Range<T>> for RangeFull {
    type Error = RangeConversionError;

    fn try_from(value: Range<T>) -> Result<Self, Self::Error> {
        match value {
            Range::Nonempty {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            } => Ok(..),
            _ => Err(RangeConversionError::new("RangeFull")),
        }
    }
}

impl<'a, T: FromSql<'a>> FromSql<'a> for Range<T> {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let member_type = match *ty.kind() {
            Kind::Range(ref member) => member,
            _ => panic!("expected range type"),
        };

        postgres_protocol::types::range_from_sql(raw).and_then(|r| {
            Ok(match r {
                postgres_protocol::types::Range::Empty => Range::Empty,
                postgres_protocol::types::Range::Nonempty(lower, upper) => Range::Nonempty {
                    lower: match lower {
                        RangeBound::Inclusive(Some(lower)) => {
                            Bound::Included(T::from_sql(member_type, lower)?)
                        }
                        RangeBound::Exclusive(Some(lower)) => {
                            Bound::Excluded(T::from_sql(member_type, lower)?)
                        }
                        _ => Bound::Unbounded,
                    },
                    upper: match upper {
                        RangeBound::Inclusive(Some(upper)) => {
                            Bound::Included(T::from_sql(member_type, upper)?)
                        }
                        RangeBound::Exclusive(Some(upper)) => {
                            Bound::Excluded(T::from_sql(member_type, upper)?)
                        }
                        _ => Bound::Unbounded,
                    },
                },
            })
        })
    }

    fn accepts(ty: &Type) -> bool {
        match *ty.kind() {
            Kind::Range(ref inner) => T::accepts(inner),
            _ => false,
        }
    }
}

impl<T: ToSql> ToSql for Range<T> {
    fn to_sql(&self, ty: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let member_type = match *ty.kind() {
            Kind::Range(ref member) => member,
            _ => panic!("expected range type"),
        };

        match self {
            Range::Empty => {
                postgres_protocol::types::empty_range_to_sql(w);
                Ok(())
            }
            Range::Nonempty { lower, upper } => postgres_protocol::types::range_to_sql(
                |w| match lower {
                    Bound::Included(lower) => lower
                        .to_sql(member_type, w)
                        .map(postgres_protocol::IsNull::from)
                        .map(RangeBound::Inclusive),
                    Bound::Excluded(lower) => lower
                        .to_sql(member_type, w)
                        .map(postgres_protocol::IsNull::from)
                        .map(RangeBound::Exclusive),
                    Bound::Unbounded => Ok(RangeBound::Unbounded),
                },
                |w| match upper {
                    Bound::Included(upper) => upper
                        .to_sql(member_type, w)
                        .map(postgres_protocol::IsNull::from)
                        .map(RangeBound::Inclusive),
                    Bound::Excluded(upper) => upper
                        .to_sql(member_type, w)
                        .map(postgres_protocol::IsNull::from)
                        .map(RangeBound::Exclusive),
                    Bound::Unbounded => Ok(RangeBound::Unbounded),
                },
                w,
            ),
        }
        .map(|_| IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        match *ty.kind() {
            Kind::Range(ref inner) => <&T as ToSql>::accepts(inner),
            _ => false,
        }
    }

    to_sql_checked!();
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use super::Range;

    #[test]
    fn from_std_range() {
        let range: Range<i32> = (1..10).into();
        match range {
            Range::Nonempty { lower, upper } => {
                assert!(matches!(lower, Bound::Included(1)));
                assert!(matches!(upper, Bound::Excluded(10)));
            }
            Range::Empty => panic!("unexpected empty range"),
        }
    }

    #[test]
    fn from_std_range_inclusive() {
        let range: Range<i32> = (1..=10).into();
        match range {
            Range::Nonempty { lower, upper } => {
                assert!(matches!(lower, Bound::Included(1)));
                assert!(matches!(upper, Bound::Included(10)));
            }
            Range::Empty => panic!("unexpected empty range"),
        }
    }

    #[test]
    fn from_std_range_from() {
        let range: Range<i32> = (1..).into();
        match range {
            Range::Nonempty { lower, upper } => {
                assert!(matches!(lower, Bound::Included(1)));
                assert!(matches!(upper, Bound::Unbounded));
            }
            Range::Empty => panic!("unexpected empty range"),
        }
    }

    #[test]
    fn from_std_range_to() {
        let range: Range<i32> = (..10).into();
        match range {
            Range::Nonempty { lower, upper } => {
                assert!(matches!(lower, Bound::Unbounded));
                assert!(matches!(upper, Bound::Excluded(10)));
            }
            Range::Empty => panic!("unexpected empty range"),
        }
    }

    #[test]
    fn from_std_range_to_inclusive() {
        let range: Range<i32> = (..=10).into();
        match range {
            Range::Nonempty { lower, upper } => {
                assert!(matches!(lower, Bound::Unbounded));
                assert!(matches!(upper, Bound::Included(10)));
            }
            Range::Empty => panic!("unexpected empty range"),
        }
    }

    #[test]
    fn from_std_range_full() {
        let range: Range<i32> = (..).into();
        match range {
            Range::Nonempty { lower, upper } => {
                assert!(matches!(lower, Bound::Unbounded));
                assert!(matches!(upper, Bound::Unbounded));
            }
            Range::Empty => panic!("unexpected empty range"),
        }
    }

    #[test]
    fn into_std_range() {
        let range = Range::Nonempty {
            lower: Bound::Included(1),
            upper: Bound::Excluded(10),
        };
        let std_range = std::ops::Range::<i32>::try_from(range).unwrap();
        assert_eq!(std_range, 1..10);
    }

    #[test]
    fn into_std_range_inclusive() {
        let range = Range::Nonempty {
            lower: Bound::Included(1),
            upper: Bound::Included(10),
        };
        let std_range = std::ops::RangeInclusive::<i32>::try_from(range).unwrap();
        assert_eq!(std_range, 1..=10);
    }

    #[test]
    fn into_std_range_from() {
        let range = Range::Nonempty {
            lower: Bound::Included(1),
            upper: Bound::Unbounded,
        };
        let std_range = std::ops::RangeFrom::<i32>::try_from(range).unwrap();
        assert_eq!(std_range.start, 1);
    }

    #[test]
    fn into_std_range_to() {
        let range = Range::Nonempty {
            lower: Bound::Unbounded,
            upper: Bound::Excluded(10),
        };
        let std_range = std::ops::RangeTo::<i32>::try_from(range).unwrap();
        assert_eq!(std_range.end, 10);
    }

    #[test]
    fn into_std_range_to_inclusive() {
        let range = Range::Nonempty {
            lower: Bound::Unbounded,
            upper: Bound::Included(10),
        };
        let std_range = std::ops::RangeToInclusive::<i32>::try_from(range).unwrap();
        assert_eq!(std_range.end, 10);
    }

    #[test]
    fn into_std_range_full() {
        let range: Range<i32> = Range::Nonempty {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        };
        std::ops::RangeFull::try_from(range).unwrap();
    }

    #[test]
    fn into_std_range_error() {
        let range = Range::Nonempty {
            lower: Bound::Included(1),
            upper: Bound::Included(10),
        };
        assert!(std::ops::Range::<i32>::try_from(range).is_err());
    }
}
