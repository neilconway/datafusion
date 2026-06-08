// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! This file contains the implementation of the `JoinHashMap` struct, which
//! is used to store the mapping between hash values based on the build side
//! ["on" values] to a list of indices with this key's value.

use std::fmt::{self, Debug};
use std::ops::Range;
use std::ops::Sub;

use arrow::array::BooleanArray;
use arrow::buffer::BooleanBuffer;
use arrow::datatypes::ArrowNativeType;
use hashbrown::HashTable;
use hashbrown::hash_table::Entry::{Occupied, Vacant};

use crate::joins::utils::JoinKeyComparator;

/// Maps a `u64` hash value based on the build side ["on" values] to a list of indices with this key's value.
///
/// By allocating a `HashMap` with capacity for *at least* the number of rows for entries at the build side,
/// we make sure that we don't have to re-hash the hashmap, which needs access to the key (the hash in this case) value.
///
/// E.g. 1 -> [3, 6, 8] indicates that the column values map to rows 3, 6 and 8 for hash value 1
/// As the key is a hash value, we need to check possible hash collisions in the probe stage
/// During this stage it might be the case that a row is contained the same hashmap value,
/// but the values don't match. Those are checked in the `equal_rows_arr` method.
///
/// The indices (values) are stored in a separate chained list stored as `Vec<u32>` or `Vec<u64>`.
///
/// The first value (+1) is stored in the hashmap, whereas the next value is stored in array at the position value.
///
/// The chain can be followed until the value "0" has been reached, meaning the end of the list.
/// Also see chapter 5.3 of [Balancing vectorized query execution with bandwidth-optimized storage](https://dare.uva.nl/search?identifier=5ccbb60a-38b8-4eeb-858a-e7735dd37487)
///
/// # Example
///
/// ``` text
/// See the example below:
///
/// Insert (10,1)            <-- insert hash value 10 with row index 1
/// map:
/// ----------
/// | 10 | 2 |
/// ----------
/// next:
/// ---------------------
/// | 0 | 0 | 0 | 0 | 0 |
/// ---------------------
/// Insert (20,2)
/// map:
/// ----------
/// | 10 | 2 |
/// | 20 | 3 |
/// ----------
/// next:
/// ---------------------
/// | 0 | 0 | 0 | 0 | 0 |
/// ---------------------
/// Insert (10,3)           <-- collision! row index 3 has a hash value of 10 as well
/// map:
/// ----------
/// | 10 | 4 |
/// | 20 | 3 |
/// ----------
/// next:
/// ---------------------
/// | 0 | 0 | 0 | 2 | 0 |  <--- hash value 10 maps to 4,2 (which means indices values 3,1)
/// ---------------------
/// Insert (10,4)          <-- another collision! row index 4 ALSO has a hash value of 10
/// map:
/// ---------
/// | 10 | 5 |
/// | 20 | 3 |
/// ---------
/// next:
/// ---------------------
/// | 0 | 0 | 0 | 2 | 4 | <--- hash value 10 maps to 5,4,2 (which means indices values 4,3,1)
/// ---------------------
/// ```
///
/// Here we have an option between creating a `JoinHashMapType` using `u32` or `u64` indices
/// based on how many rows were being used for indices.
///
/// At runtime we choose between using `JoinHashMapU32` and `JoinHashMapU64` which oth implement
/// `JoinHashMapType`.
///
/// ## Note on use of this trait as a public API
/// This is currently a public trait but is mainly intended for internal use within DataFusion.
/// For example, we may compare references to `JoinHashMapType` implementations by pointer equality
/// rather than deep equality of contents, as deep equality would be expensive and in our usage
/// patterns it is impossible for two different hash maps to have identical contents in a practical sense.
pub trait JoinHashMapType: Send + Sync {
    fn extend_zero(&mut self, len: usize);

    fn update_from_iter<'a>(
        &mut self,
        iter: Box<dyn Iterator<Item = (usize, &'a u64)> + Send + 'a>,
        deleted_offset: usize,
    );

    fn get_matched_indices<'a>(
        &self,
        iter: Box<dyn Iterator<Item = (usize, &'a u64)> + 'a>,
        deleted_offset: Option<usize>,
    ) -> (Vec<u32>, Vec<u64>);

    fn get_matched_indices_with_limit_offset(
        &self,
        hash_values: &[u64],
        limit: usize,
        offset: MapOffset,
        input_indices: &mut Vec<u32>,
        match_indices: &mut Vec<u64>,
    ) -> Option<MapOffset>;

    /// Scans probe rows in `range` and appends each matching probe index at most once.
    ///
    /// `comparator` is consulted for each candidate build/probe pair that has a
    /// matching hash value. Once it reports equal join keys for a probe row,
    /// that probe index is appended and the remaining build-side chain for that
    /// probe row is skipped.
    fn get_probe_indices_with_any_match(
        &self,
        hash_values: &[u64],
        range: Range<usize>,
        comparator: &JoinKeyComparator,
        probe_indices: &mut Vec<u32>,
    );

    /// Returns a BooleanArray indicating which of the provided hashes exist in the map.
    fn contain_hashes(&self, hash_values: &[u64]) -> BooleanArray;

    /// Returns `true` if the join hash map contains no entries.
    fn is_empty(&self) -> bool;

    /// Returns the number of entries in the join hash map.
    fn len(&self) -> usize;
}

pub struct JoinHashMapU32 {
    // Stores hash value to last row index
    map: HashTable<(u64, u32)>,
    // Stores indices in chained list data structure
    next: Vec<u32>,
}

impl JoinHashMapU32 {
    #[cfg(test)]
    pub(crate) fn new(map: HashTable<(u64, u32)>, next: Vec<u32>) -> Self {
        Self { map, next }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            map: HashTable::with_capacity(cap),
            next: vec![0; cap],
        }
    }
}

impl Debug for JoinHashMapU32 {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        Ok(())
    }
}

impl JoinHashMapType for JoinHashMapU32 {
    fn extend_zero(&mut self, _: usize) {}

    fn update_from_iter<'a>(
        &mut self,
        iter: Box<dyn Iterator<Item = (usize, &'a u64)> + Send + 'a>,
        deleted_offset: usize,
    ) {
        update_from_iter::<u32>(&mut self.map, &mut self.next, iter, deleted_offset);
    }

    fn get_matched_indices<'a>(
        &self,
        iter: Box<dyn Iterator<Item = (usize, &'a u64)> + 'a>,
        deleted_offset: Option<usize>,
    ) -> (Vec<u32>, Vec<u64>) {
        get_matched_indices::<u32>(&self.map, &self.next, iter, deleted_offset)
    }

    fn get_matched_indices_with_limit_offset(
        &self,
        hash_values: &[u64],
        limit: usize,
        offset: MapOffset,
        input_indices: &mut Vec<u32>,
        match_indices: &mut Vec<u64>,
    ) -> Option<MapOffset> {
        get_matched_indices_with_limit_offset::<u32>(
            &self.map,
            &self.next,
            hash_values,
            limit,
            offset,
            input_indices,
            match_indices,
        )
    }

    fn get_probe_indices_with_any_match(
        &self,
        hash_values: &[u64],
        range: Range<usize>,
        comparator: &JoinKeyComparator,
        probe_indices: &mut Vec<u32>,
    ) {
        get_probe_indices_with_any_match(
            &self.map,
            &self.next,
            hash_values,
            range,
            |build_idx, probe_idx| {
                comparator.is_equal(build_idx as usize, probe_idx as usize)
            },
            probe_indices,
        )
    }

    fn contain_hashes(&self, hash_values: &[u64]) -> BooleanArray {
        contain_hashes(&self.map, hash_values)
    }

    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    fn len(&self) -> usize {
        self.map.len()
    }
}

pub struct JoinHashMapU64 {
    // Stores hash value to last row index
    map: HashTable<(u64, u64)>,
    // Stores indices in chained list data structure
    next: Vec<u64>,
}

impl JoinHashMapU64 {
    #[cfg(test)]
    pub(crate) fn new(map: HashTable<(u64, u64)>, next: Vec<u64>) -> Self {
        Self { map, next }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            map: HashTable::with_capacity(cap),
            next: vec![0; cap],
        }
    }
}

impl Debug for JoinHashMapU64 {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        Ok(())
    }
}

impl JoinHashMapType for JoinHashMapU64 {
    fn extend_zero(&mut self, _: usize) {}

    fn update_from_iter<'a>(
        &mut self,
        iter: Box<dyn Iterator<Item = (usize, &'a u64)> + Send + 'a>,
        deleted_offset: usize,
    ) {
        update_from_iter::<u64>(&mut self.map, &mut self.next, iter, deleted_offset);
    }

    fn get_matched_indices<'a>(
        &self,
        iter: Box<dyn Iterator<Item = (usize, &'a u64)> + 'a>,
        deleted_offset: Option<usize>,
    ) -> (Vec<u32>, Vec<u64>) {
        get_matched_indices::<u64>(&self.map, &self.next, iter, deleted_offset)
    }

    fn get_matched_indices_with_limit_offset(
        &self,
        hash_values: &[u64],
        limit: usize,
        offset: MapOffset,
        input_indices: &mut Vec<u32>,
        match_indices: &mut Vec<u64>,
    ) -> Option<MapOffset> {
        get_matched_indices_with_limit_offset::<u64>(
            &self.map,
            &self.next,
            hash_values,
            limit,
            offset,
            input_indices,
            match_indices,
        )
    }

    fn get_probe_indices_with_any_match(
        &self,
        hash_values: &[u64],
        range: Range<usize>,
        comparator: &JoinKeyComparator,
        probe_indices: &mut Vec<u32>,
    ) {
        get_probe_indices_with_any_match(
            &self.map,
            &self.next,
            hash_values,
            range,
            |build_idx, probe_idx| {
                comparator.is_equal(build_idx as usize, probe_idx as usize)
            },
            probe_indices,
        )
    }

    fn contain_hashes(&self, hash_values: &[u64]) -> BooleanArray {
        contain_hashes(&self.map, hash_values)
    }

    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    fn len(&self) -> usize {
        self.map.len()
    }
}

use crate::joins::MapOffset;
use crate::joins::chain::traverse_chain;

pub fn update_from_iter<'a, T>(
    map: &mut HashTable<(u64, T)>,
    next: &mut [T],
    iter: Box<dyn Iterator<Item = (usize, &'a u64)> + Send + 'a>,
    deleted_offset: usize,
) where
    T: Copy + TryFrom<usize> + PartialOrd,
    <T as TryFrom<usize>>::Error: Debug,
{
    for (row, &hash_value) in iter {
        let entry = map.entry(
            hash_value,
            |&(hash, _)| hash_value == hash,
            |&(hash, _)| hash,
        );

        match entry {
            Occupied(mut occupied_entry) => {
                // Already exists: add index to next array
                let (_, index) = occupied_entry.get_mut();
                let prev_index = *index;
                // Store new value inside hashmap
                *index = T::try_from(row + 1).unwrap();
                // Update chained Vec at `row` with previous value
                next[row - deleted_offset] = prev_index;
            }
            Vacant(vacant_entry) => {
                vacant_entry.insert((hash_value, T::try_from(row + 1).unwrap()));
            }
        }
    }
}

pub fn get_matched_indices<'a, T>(
    map: &HashTable<(u64, T)>,
    next: &[T],
    iter: Box<dyn Iterator<Item = (usize, &'a u64)> + 'a>,
    deleted_offset: Option<usize>,
) -> (Vec<u32>, Vec<u64>)
where
    T: Copy + TryFrom<usize> + PartialOrd + Into<u64> + Sub<Output = T>,
    <T as TryFrom<usize>>::Error: Debug,
{
    let mut input_indices = vec![];
    let mut match_indices = vec![];
    let zero = T::try_from(0).unwrap();
    let one = T::try_from(1).unwrap();

    for (row_idx, hash_value) in iter {
        // Get the hash and find it in the index
        if let Some((_, index)) = map.find(*hash_value, |(hash, _)| *hash_value == *hash)
        {
            let mut i = *index - one;
            loop {
                let match_row_idx = if let Some(offset) = deleted_offset {
                    let offset = T::try_from(offset).unwrap();
                    // This arguments means that we prune the next index way before here.
                    if i < offset {
                        // End of the list due to pruning
                        break;
                    }
                    i - offset
                } else {
                    i
                };
                match_indices.push(match_row_idx.into());
                input_indices.push(row_idx as u32);
                // Follow the chain to get the next index value
                let next_chain = next[match_row_idx.into() as usize];
                if next_chain == zero {
                    // end of list
                    break;
                }
                i = next_chain - one;
            }
        }
    }

    (input_indices, match_indices)
}

pub fn get_matched_indices_with_limit_offset<T>(
    map: &HashTable<(u64, T)>,
    next_chain: &[T],
    hash_values: &[u64],
    limit: usize,
    offset: MapOffset,
    input_indices: &mut Vec<u32>,
    match_indices: &mut Vec<u64>,
) -> Option<MapOffset>
where
    T: Copy + TryFrom<usize> + PartialOrd + Into<u64> + Sub<Output = T>,
    <T as TryFrom<usize>>::Error: Debug,
    T: ArrowNativeType,
{
    // Clear the buffer before producing new results
    input_indices.clear();
    match_indices.clear();
    let one = T::try_from(1).unwrap();

    // Check if hashmap consists of unique values
    // If so, we can skip the chain traversal
    if map.len() == next_chain.len() {
        let start = offset.0;
        let end = (start + limit).min(hash_values.len());
        for (i, &hash) in hash_values[start..end].iter().enumerate() {
            if let Some((_, idx)) = map.find(hash, |(h, _)| hash == *h) {
                input_indices.push(start as u32 + i as u32);
                match_indices.push((*idx - one).into());
            }
        }
        return if end == hash_values.len() {
            None
        } else {
            Some((end, None))
        };
    }

    let mut remaining_output = limit;

    // Calculate initial `hash_values` index before iterating
    let to_skip = match offset {
        // None `initial_next_idx` indicates that `initial_idx` processing hasn't been started
        (idx, None) => idx,
        // Zero `initial_next_idx` indicates that `initial_idx` has been processed during
        // previous iteration, and it should be skipped
        (idx, Some(0)) => idx + 1,
        // Otherwise, process remaining `initial_idx` matches by traversing `next_chain`,
        // to start with the next index
        (idx, Some(next_idx)) => {
            let next_idx: T = T::usize_as(next_idx as usize);
            let is_last = idx == hash_values.len() - 1;
            if let Some(next_offset) = traverse_chain(
                next_chain,
                idx,
                next_idx,
                &mut remaining_output,
                input_indices,
                match_indices,
                is_last,
            ) {
                return Some(next_offset);
            }
            idx + 1
        }
    };

    let hash_values_len = hash_values.len();
    for (i, &hash) in hash_values[to_skip..].iter().enumerate() {
        let row_idx = to_skip + i;
        if let Some((_, idx)) = map.find(hash, |(h, _)| hash == *h) {
            let idx: T = *idx;
            let is_last = row_idx == hash_values_len - 1;
            if let Some(next_offset) = traverse_chain(
                next_chain,
                row_idx,
                idx,
                &mut remaining_output,
                input_indices,
                match_indices,
                is_last,
            ) {
                return Some(next_offset);
            }
        }
    }
    None
}

pub fn get_probe_indices_with_any_match<T, F>(
    map: &HashTable<(u64, T)>,
    next_chain: &[T],
    hash_values: &[u64],
    range: Range<usize>,
    mut predicate: F,
    probe_indices: &mut Vec<u32>,
) where
    T: Copy + TryFrom<usize> + PartialOrd + Into<u64> + Sub<Output = T>,
    <T as TryFrom<usize>>::Error: Debug,
    F: FnMut(u64, u32) -> bool,
{
    probe_indices.clear();

    let end = range.end.min(hash_values.len());
    if range.start >= end {
        return;
    }

    let zero = T::try_from(0).unwrap();
    let one = T::try_from(1).unwrap();

    for (probe_idx, &hash) in hash_values.iter().enumerate().take(end).skip(range.start) {
        let Some((_, index)) = map.find(hash, |(h, _)| hash == *h) else {
            continue;
        };

        let mut build_idx = *index - one;
        loop {
            let build_idx_u64 = build_idx.into();
            if predicate(build_idx_u64, probe_idx as u32) {
                probe_indices.push(probe_idx as u32);
                break;
            }

            let next = next_chain[build_idx_u64 as usize];
            if next == zero {
                break;
            }
            build_idx = next - one;
        }
    }
}

pub fn contain_hashes<T>(map: &HashTable<(u64, T)>, hash_values: &[u64]) -> BooleanArray {
    let buffer = BooleanBuffer::collect_bool(hash_values.len(), |i| {
        let hash = hash_values[i];
        map.find(hash, |(h, _)| hash == *h).is_some()
    });
    BooleanArray::new(buffer, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::Result;

    #[test]
    fn test_contain_hashes() {
        let mut hash_map = JoinHashMapU32::with_capacity(10);
        hash_map.update_from_iter(Box::new([10u64, 20u64, 30u64].iter().enumerate()), 0);

        let probe_hashes = vec![10, 11, 20, 21, 30, 31];
        let array = hash_map.contain_hashes(&probe_hashes);

        assert_eq!(array.len(), probe_hashes.len());

        for (i, &hash) in probe_hashes.iter().enumerate() {
            if matches!(hash, 10 | 20 | 30) {
                assert!(array.value(i), "Hash {hash} should exist in the map");
            } else {
                assert!(!array.value(i), "Hash {hash} should NOT exist in the map");
            }
        }
    }

    #[test]
    fn probe_indices_with_any_match_are_sorted_and_unique() -> Result<()> {
        let mut hash_map = JoinHashMapU32::with_capacity(5);
        hash_map
            .update_from_iter(Box::new([10u64, 10, 20, 30, 10].iter().enumerate()), 0);

        let probe_hashes = vec![10, 11, 20, 10, 30, 40];
        let mut probe_indices = vec![];
        get_probe_indices_with_any_match(
            &hash_map.map,
            &hash_map.next,
            &probe_hashes,
            0..probe_hashes.len(),
            |_build_idx, _probe_idx| true,
            &mut probe_indices,
        );

        assert_eq!(probe_indices, vec![0, 2, 3, 4]);
        Ok(())
    }

    #[test]
    fn probe_indices_with_any_match_continue_after_false_predicate() -> Result<()> {
        let mut hash_map = JoinHashMapU32::with_capacity(2);
        hash_map.update_from_iter(Box::new([10u64, 10].iter().enumerate()), 0);

        let probe_hashes = vec![10];
        let mut visited_build_indices = vec![];
        let mut probe_indices = vec![];
        get_probe_indices_with_any_match(
            &hash_map.map,
            &hash_map.next,
            &probe_hashes,
            0..probe_hashes.len(),
            |build_idx, _probe_idx| {
                visited_build_indices.push(build_idx);
                build_idx == 0
            },
            &mut probe_indices,
        );

        assert_eq!(visited_build_indices, vec![1, 0]);
        assert_eq!(probe_indices, vec![0]);
        Ok(())
    }

    #[test]
    fn probe_indices_with_any_match_u64_matches_and_omits_no_match_probes() -> Result<()>
    {
        let mut hash_map = JoinHashMapU64::with_capacity(2);
        hash_map.update_from_iter(Box::new([10u64, 20].iter().enumerate()), 0);

        let probe_hashes = vec![30, 10, 40, 20];
        let mut probe_indices = vec![99];
        get_probe_indices_with_any_match(
            &hash_map.map,
            &hash_map.next,
            &probe_hashes,
            0..probe_hashes.len(),
            |_build_idx, _probe_idx| true,
            &mut probe_indices,
        );

        assert_eq!(probe_indices, vec![1, 3]);
        Ok(())
    }
}
