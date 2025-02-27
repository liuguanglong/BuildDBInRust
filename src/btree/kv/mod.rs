use super::*;

pub mod node;
pub mod memorycontext;
pub mod windowsfilecontext;
pub mod nodeinterface;
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
    NotDataBaseFile,
    LoadDataException,
    
    NodeNotFound,
    RootNotFound,
    
    CreateReaderError,
}

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
            ContextError::NotDataBaseFile => write!(f,"Exception occured when trying to load database file! It's not a database file!"),
            ContextError::LoadDataException => write!(f,"Exception occured when trying to load database file! File content is wrong!"),
            ContextError::RootNotFound => write!(f,"Exception occured when trying to get root node!"),
            ContextError::NodeNotFound => write!(f,"Exception occured when trying to get node!"),
            ContextError::CreateReaderError => write!(f,"Get Reader Error!"),
        }
    }
}

pub const BTREE_PAGE_SIZE:usize = 4096;
pub const BNODE_FREE_LIST: u16 = 3;
pub const FREE_LIST_HEADER: usize = 4 + 8 + 8;
pub const FREE_LIST_CAP: usize = (BTREE_PAGE_SIZE - FREE_LIST_HEADER) / 8;
pub const FREE_LIST_CAP_WITH_VERSION: usize = (BTREE_PAGE_SIZE - FREE_LIST_HEADER) / 16;
pub const DB_SIG:&[u8] = "BuildYourOwnDB24".as_bytes();