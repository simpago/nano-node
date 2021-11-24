use crate::utils::TomlWriter;
use anyhow::Result;
use std::ffi::c_void;

type TomlPutU64Callback =
    unsafe extern "C" fn(*mut c_void, *const u8, usize, u64, *const u8, usize) -> i32;

type TomlPutI64Callback =
    unsafe extern "C" fn(*mut c_void, *const u8, usize, i64, *const u8, usize) -> i32;

type TomlPutF64Callback =
    unsafe extern "C" fn(*mut c_void, *const u8, usize, f64, *const u8, usize) -> i32;

type TomlPutStrCallback =
    unsafe extern "C" fn(*mut c_void, *const u8, usize, *const u8, usize, *const u8, usize) -> i32;

type TomlPutBoolCallback =
    unsafe extern "C" fn(*mut c_void, *const u8, usize, bool, *const u8, usize) -> i32;

static mut PUT_U64_CALLBACK: Option<TomlPutU64Callback> = None;
static mut PUT_I64_CALLBACK: Option<TomlPutI64Callback> = None;
static mut PUT_F64_CALLBACK: Option<TomlPutF64Callback> = None;
static mut PUT_STR_CALLBACK: Option<TomlPutStrCallback> = None;
static mut PUT_BOOL_CALLBACK: Option<TomlPutBoolCallback> = None;

#[no_mangle]
pub unsafe extern "C" fn rsn_callback_toml_put_u64(f: TomlPutU64Callback) {
    PUT_U64_CALLBACK = Some(f);
}

#[no_mangle]
pub unsafe extern "C" fn rsn_callback_toml_put_i64(f: TomlPutI64Callback) {
    PUT_I64_CALLBACK = Some(f);
}

#[no_mangle]
pub unsafe extern "C" fn rsn_callback_toml_put_f64(f: TomlPutF64Callback) {
    PUT_F64_CALLBACK = Some(f);
}

#[no_mangle]
pub unsafe extern "C" fn rsn_callback_toml_put_str(f: TomlPutStrCallback) {
    PUT_STR_CALLBACK = Some(f);
}

#[no_mangle]
pub unsafe extern "C" fn rsn_callback_toml_put_bool(f: TomlPutBoolCallback) {
    PUT_BOOL_CALLBACK = Some(f);
}

pub struct FfiToml {
    handle: *mut c_void,
}

impl FfiToml {
    pub fn new(handle: *mut c_void) -> Self {
        Self { handle }
    }
}

impl TomlWriter for FfiToml {
    fn put_u16(&mut self, key: &str, value: u16, documentation: &str) -> Result<()> {
        self.put_u64(key, value as u64, documentation)
    }

    fn put_u32(&mut self, key: &str, value: u32, documentation: &str) -> Result<()> {
        self.put_u64(key, value as u64, documentation)
    }

    fn put_u64(&mut self, key: &str, value: u64, documentation: &str) -> Result<()> {
        unsafe {
            match PUT_U64_CALLBACK {
                Some(f) => {
                    if f(
                        self.handle,
                        key.as_ptr(),
                        key.bytes().len(),
                        value,
                        documentation.as_ptr(),
                        documentation.as_bytes().len(),
                    ) == 0
                    {
                        Ok(())
                    } else {
                        Err(anyhow!("PUT_U32_CALLBACK returned error"))
                    }
                }
                None => Err(anyhow!("PUT_U32_CALLBACK not set")),
            }
        }
    }

    fn put_i64(&mut self, key: &str, value: i64, documentation: &str) -> Result<()> {
        unsafe {
            match PUT_I64_CALLBACK {
                Some(f) => {
                    if f(
                        self.handle,
                        key.as_ptr(),
                        key.bytes().len(),
                        value,
                        documentation.as_ptr(),
                        documentation.as_bytes().len(),
                    ) == 0
                    {
                        Ok(())
                    } else {
                        Err(anyhow!("PUT_I64_CALLBACK returned error"))
                    }
                }
                None => Err(anyhow!("PUT_I64_CALLBACK not set")),
            }
        }
    }

    fn put_str(&mut self, key: &str, value: &str, documentation: &str) -> Result<()> {
        unsafe {
            match PUT_STR_CALLBACK {
                Some(f) => {
                    if f(
                        self.handle,
                        key.as_ptr(),
                        key.bytes().len(),
                        value.as_ptr(),
                        value.bytes().len(),
                        documentation.as_ptr(),
                        documentation.as_bytes().len(),
                    ) == 0
                    {
                        Ok(())
                    } else {
                        Err(anyhow!("PUT_STR_CALLBACK returned error"))
                    }
                }
                None => Err(anyhow!("PUT_STR_CALLBACK not set")),
            }
        }
    }

    fn put_bool(&mut self, key: &str, value: bool, documentation: &str) -> Result<()> {
        unsafe {
            match PUT_BOOL_CALLBACK {
                Some(f) => {
                    if f(
                        self.handle,
                        key.as_ptr(),
                        key.bytes().len(),
                        value,
                        documentation.as_ptr(),
                        documentation.as_bytes().len(),
                    ) == 0
                    {
                        Ok(())
                    } else {
                        Err(anyhow!("PUT_BOOL_CALLBACK returned error"))
                    }
                }
                None => Err(anyhow!("PUT_BOOL_CALLBACK not set")),
            }
        }
    }

    fn put_usize(&mut self, key: &str, value: usize, documentation: &str) -> Result<()> {
        self.put_u64(key, value as u64, documentation)
    }

    fn put_f64(&mut self, key: &str, value: f64, documentation: &str) -> Result<()> {
        unsafe {
            match PUT_F64_CALLBACK {
                Some(f) => {
                    if f(
                        self.handle,
                        key.as_ptr(),
                        key.bytes().len(),
                        value,
                        documentation.as_ptr(),
                        documentation.as_bytes().len(),
                    ) == 0
                    {
                        Ok(())
                    } else {
                        Err(anyhow!("PUT_F64_CALLBACK returned error"))
                    }
                }
                None => Err(anyhow!("PUT_F64_CALLBACK not set")),
            }
        }
    }
}
