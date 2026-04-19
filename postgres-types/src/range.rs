use std::{error::Error, ops::Bound};

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
