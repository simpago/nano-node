use super::WorkRng;
use rand::Rng;

pub(crate) struct XorShift1024Star {
    s: [u64; 16],
    p: u32,
}

impl XorShift1024Star {
    pub fn new() -> Self {
        Self {
            s: rand::rng().random(),
            p: 0,
        }
    }
    pub fn next(&mut self) -> u64 {
        let p_l = self.p;
        let pn = p_l.wrapping_add(1) & 15;
        self.p = pn;
        let mut s0 = self.s[p_l as usize];
        let mut s1 = self.s[pn as usize];
        s1 ^= s1 << 31; //a
        s1 ^= s1 >> 11; //b
        s0 ^= s0 >> 30; //c
        let x = s0 ^ s1;
        self.s[pn as usize] = x;
        x.wrapping_mul(1181783497276652981u64)
    }
}

impl WorkRng for XorShift1024Star {
    fn next_work(&mut self) -> u64 {
        self.next()
    }
}
