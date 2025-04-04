use arrow::array::ArrayFromIter;
use arrow::bitmap::BitmapBuilder;

use crate::chunked_array::object::{ObjectArray, PolarsObject};

// TODO: more efficient implementations, I really took the short path here.
impl<'a, T: PolarsObject> ArrayFromIter<&'a T> for ObjectArray<T> {
    fn arr_from_iter<I: IntoIterator<Item = &'a T>>(iter: I) -> Self {
        Self::try_arr_from_iter(iter.into_iter().map(|o| -> Result<_, ()> { Ok(Some(o)) })).unwrap()
    }

    fn try_arr_from_iter<E, I: IntoIterator<Item = Result<&'a T, E>>>(iter: I) -> Result<Self, E> {
        Self::try_arr_from_iter(iter.into_iter().map(|o| Ok(Some(o?))))
    }
}

impl<'a, T: PolarsObject> ArrayFromIter<Option<&'a T>> for ObjectArray<T> {
    fn arr_from_iter<I: IntoIterator<Item = Option<&'a T>>>(iter: I) -> Self {
        Self::try_arr_from_iter(iter.into_iter().map(|o| -> Result<_, ()> { Ok(o) })).unwrap()
    }

    fn try_arr_from_iter<E, I: IntoIterator<Item = Result<Option<&'a T>, E>>>(
        iter: I,
    ) -> Result<Self, E> {
        let iter = iter.into_iter();
        let size = iter.size_hint().0;

        let mut null_mask_builder = BitmapBuilder::with_capacity(size);
        let values: Vec<T> = iter
            .map(|value| match value? {
                Some(value) => {
                    null_mask_builder.push(true);
                    Ok(value.clone())
                },
                None => {
                    null_mask_builder.push(false);
                    Ok(T::default())
                },
            })
            .collect::<Result<Vec<T>, E>>()?;

        Ok(ObjectArray::from(values).with_validity(null_mask_builder.into_opt_validity()))
    }
}
