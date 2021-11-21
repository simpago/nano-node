use crate::config::{LmdbConfig, SyncStrategy};

#[repr(C)]
pub struct LmdbConfigDto {
    pub sync: u8,
    pub max_databases: u32,
    pub map_size: usize,
}

#[no_mangle]
pub unsafe extern "C" fn rsn_lmdb_config_create(dto: *mut LmdbConfigDto) {
    let config = LmdbConfig::new();
    let dto = &mut (*dto);
    dto.sync = match config.sync {
        SyncStrategy::Always => 0,
        SyncStrategy::NosyncSafe => 1,
        SyncStrategy::NosyncUnsafe => 2,
        SyncStrategy::NosyncUnsafeLargeMemory => 3,
    };
    dto.max_databases = config.max_databases;
    dto.map_size = config.map_size;
}
