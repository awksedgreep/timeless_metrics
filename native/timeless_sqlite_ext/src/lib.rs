use std::ffi::{c_char, c_int};

use rusqlite::ffi;
use rusqlite::{Connection, Result};

unsafe extern "C" fn init_common(
    db: *mut ffi::sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    Connection::extension_init2(db, pz_err_msg, p_api, extension_init)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_extension_init(
    db: *mut ffi::sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    init_common(db, pz_err_msg, p_api)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_timelesssqliteext_init(
    db: *mut ffi::sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    init_common(db, pz_err_msg, p_api)
}

fn extension_init(db: Connection) -> Result<bool> {
    timeless_ext::register_telemetry(&db)?;
    Ok(false)
}
