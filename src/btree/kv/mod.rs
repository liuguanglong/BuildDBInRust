use super::*;

pub mod node;
pub mod memorycontext;
pub mod windowsfilecontext;
pub mod nodeinterface;
pub mod noderef;
pub mod contextinterface;

#[derive(Debug)]
pub enum ContextError{
    OpenFileError,
    GetFileSizeError,
    CreateNTSectionError,
    MapSectionViewError,
    ExtendNTSectionError,
    FlushViewofFileError,
    FlushFileBUffersError,

}

// 实现 fmt::Display 特征
impl fmt::Display for ContextError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ContextError::OpenFileError => write!(f,"Exception occured when trying to open file!"),
            ContextError::GetFileSizeError => write!(f,"Exception occured when trying to get file size!"),
            ContextError::CreateNTSectionError => write!(f,"Exception occured when trying to create nt section for mapping!"),
            ContextError::MapSectionViewError => write!(f,"Exception occured when trying to map net section!"),
            ContextError::ExtendNTSectionError => write!(f,"Exception occured when trying to extend net section!"),
            ContextError::FlushViewofFileError => write!(f,"Exception occured when trying to flush content of mapping view!"),
            ContextError::FlushFileBUffersError => write!(f,"Exception occured when trying to flush content of file buffer!"),
        }
    }
}
