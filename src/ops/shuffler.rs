use rand::Rng;

pub trait Shuffle: Iterator + Sized {
    fn shuffle(self, buffer_size: usize) -> Shuffler<Self> {
        Shuffler::new(self, buffer_size)
    }
}

impl<T: Iterator> Shuffle for T {}

pub struct Shuffler<I: Iterator> {
    iter: I,
    buffer: Vec<I::Item>,
    buffer_size: usize,
    rng: rand::rngs::ThreadRng,
}

unsafe impl<I: Iterator> Send for Shuffler<I> {}

impl<I: Iterator> Shuffler<I> {
    pub fn new(iter: I, buffer_size: usize) -> Self {
        Self {
            iter,
            buffer: Vec::with_capacity(buffer_size),
            buffer_size: buffer_size,
            rng: rand::thread_rng(),
        }
    }
}

impl<I: Iterator> Iterator for Shuffler<I> {
    type Item = I::Item;
    fn next(&mut self) -> Option<Self::Item> {
        let mut num_elems = 0;

        if self.buffer.is_empty() {
            while num_elems < self.buffer_size {
                match self.iter.next() {
                    Some(v) => self.buffer.push(v),
                    None => break,
                }
                num_elems += 1;
            }
        }

        if self.buffer.len() == self.buffer_size {
            match self.iter.next() {
                Some(mut v) => {
                    let index: usize = self.rng.gen_range(0..self.buffer_size);
                    std::mem::swap(&mut v, &mut self.buffer[index]);
                    Some(v)
                }
                None => {
                    let index = self.rng.gen_range(0..self.buffer_size);
                    Some(self.buffer.remove(index))
                }
            }
        } else {
            if self.buffer.is_empty() {
                None
            } else {
                let index = self.rng.gen_range(0..self.buffer.len());
                Some(self.buffer.remove(index))
            }
        }
    }
}
