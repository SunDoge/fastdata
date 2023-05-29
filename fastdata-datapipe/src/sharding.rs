pub fn apply_sharding_filter<I>(
    iter: I,
    world_size: usize,
    rank: usize,
) -> impl Iterator<Item = I::Item>
where
    I: Iterator,
{
    iter.enumerate()
        .take_while(move |(idx, _item)| idx % world_size == rank)
        .map(|(_idx, item)| item)
}
