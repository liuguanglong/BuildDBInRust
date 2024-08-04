
use super:: shared::Shared;

#[derive(Debug)]
pub struct Mmap{
    pub ptr:*mut u8,
    pub writer:Shared<()>,
}
unsafe impl Send for Mmap {}
unsafe impl Sync for Mmap {}
