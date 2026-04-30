/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! FFM bridge for DataFusion.

use std::future::Future;
use std::slice;
use std::str;
use std::sync::Arc;
use std::sync::OnceLock;

use native_bridge_common::ffm_safe;
use parking_lot::RwLock;

use datafusion::common::DataFusionError;

use crate::api;
use crate::runtime_manager::{ExecutionMode, set_execution_mode, get_execution_mode, RuntimeManager};

static TOKIO_RUNTIME_MANAGER: RwLock<Option<Arc<RuntimeManager>>> = RwLock::new(None);

/// Stores the Java upcall function pointer for async completion callbacks.
/// The function pointer is created by Java's Linker.upcallStub() and lives
/// for the JVM process lifetime (Arena.global()).
struct AsyncCallback {
    callback: extern "C" fn(i64, i64, i32), // (listener_id, result, is_error)
}

// SAFETY: The function pointer targets a static Java method via an upcall stub
// bound to Arena.global(). It is safe to call from any thread — FFM upcalls
// do not require JVM thread attachment (unlike JNI).
unsafe impl Send for AsyncCallback {}
unsafe impl Sync for AsyncCallback {}

static ASYNC_CALLBACK: OnceLock<AsyncCallback> = OnceLock::new();

unsafe fn str_from_raw<'a>(ptr: *const u8, len: i64) -> Result<&'a str, String> {
    if ptr.is_null() {
        return Err("null string pointer".to_string());
    }
    if len < 0 {
        return Err(format!("negative string length: {}", len));
    }
    let bytes = slice::from_raw_parts(ptr, len as usize);
    str::from_utf8(bytes).map_err(|e| format!("invalid UTF-8: {}", e))
}

fn get_rt_manager() -> Result<Arc<RuntimeManager>, String> {
    TOKIO_RUNTIME_MANAGER
        .read()
        .clone()
        .ok_or_else(|| "Runtime manager not initialized".to_string())
}

fn get_async_callback() -> Result<&'static AsyncCallback, String> {
    ASYNC_CALLBACK
        .get()
        .ok_or_else(|| "Async callback not registered".to_string())
}

/// Heap-allocates an error message string as a CString and returns its pointer as i64.
/// Compatible with native_bridge_common's error convention (CString, positive pointer).
/// Note: Unlike the ffm_safe macro's into_error_ptr which negates the pointer,
/// this returns a positive pointer because the async callback uses is_error flag instead.
fn into_error_ptr(msg: String) -> i64 {
    let c = std::ffi::CString::new(msg)
        .unwrap_or_else(|_| std::ffi::CString::new("error contained null byte").unwrap());
    let ptr = c.into_raw();
    ptr as i64
}

/// Wrapper to make a raw pointer Send-safe for cross-thread dispatch.
/// SAFETY: The caller guarantees exclusive access — no concurrent calls
/// on the same pointer while the spawned task is running.
struct SendPtr(i64);
unsafe impl Send for SendPtr {}

/// Spawn on io_runtime, block calling FFM thread on oneshot channel.
fn spawn_and_wait_io<F, T>(runtime: &tokio::runtime::Runtime, task: F) -> Result<T, String>
where
    F: Future<Output = Result<T, DataFusionError>> + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = tokio::sync::oneshot::channel();
    runtime.spawn(async move {
        let _ = tx.send(task.await);
    });
    rx.blocking_recv()
        .map_err(|_| "IO task dropped before completion".to_string())?
        .map_err(|e| e.to_string())
}

/// Spawn on DedicatedExecutor (CPU), block calling FFM thread on oneshot channel.
fn spawn_and_wait_cpu<F, T>(executor: &crate::executor::DedicatedExecutor, task: F) -> Result<T, String>
where
    F: Future<Output = Result<T, DataFusionError>> + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = tokio::sync::oneshot::channel();
    // We must .await the future returned by executor.spawn() to keep the
    // internal JoinSet alive. Since we're on a sync thread, we use a
    // separate oneshot channel and just ignore the spawn future's result.
    // The trick: wrap the task so it sends the result via tx, and let the
    // executor's JoinSet handle be kept alive by spawning on io_runtime.
    let spawn_fut = executor.spawn(async move {
        let _ = tx.send(task.await);
    });
    // Drive the spawn future on a background task so the JoinSet isn't dropped
    let mgr = get_rt_manager().map_err(|e| format!("spawn_and_wait_cpu: {}", e))?;
    mgr.io_runtime.spawn(spawn_fut);
    rx.blocking_recv()
        .map_err(|_| "CPU task dropped before completion".to_string())?
        .map_err(|e| e.to_string())
}

#[no_mangle]
pub extern "C" fn df_register_async_callback(callback_ptr: *const ()) {
    let callback: extern "C" fn(i64, i64, i32) =
        unsafe { std::mem::transmute(callback_ptr) };
    match ASYNC_CALLBACK.set(AsyncCallback { callback }) {
        Ok(()) => log::info!("Async callback registered"),
        Err(_) => log::warn!("Async callback already registered, ignoring"),
    }
}

#[no_mangle]
pub extern "C" fn df_init_runtime_manager(cpu_threads: i32, execution_mode: i32) {
    let mode = ExecutionMode::from_u8(execution_mode as u8);
    set_execution_mode(mode);
    log::info!("Execution mode set to {:?} ({})", mode, execution_mode);

    let mut guard = TOKIO_RUNTIME_MANAGER.write();
    *guard = Some(Arc::new(RuntimeManager::new(cpu_threads as usize)));
}

#[no_mangle]
pub extern "C" fn df_shutdown_runtime_manager() {
    let mgr = TOKIO_RUNTIME_MANAGER.write().take();
    if let Some(mgr) = mgr {
        mgr.shutdown();
    }
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_create_global_runtime(
    memory_pool_limit: i64,
    spill_dir_ptr: *const u8,
    spill_dir_len: i64,
    spill_limit: i64,
) -> i64 {
    let spill_dir = str_from_raw(spill_dir_ptr, spill_dir_len).map_err(|e| format!("df_create_global_runtime: {}", e))?;
    api::create_global_runtime(memory_pool_limit, spill_dir, spill_limit)
        .map_err(|e| e.to_string())
}

#[no_mangle]
pub unsafe extern "C" fn df_close_global_runtime(ptr: i64) {
    api::close_global_runtime(ptr);
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_create_reader(
    table_path_ptr: *const u8,
    table_path_len: i64,
    files_ptr: *const *const u8,
    files_len_ptr: *const i64,
    files_count: i64,
) -> i64 {
    let table_path = str_from_raw(table_path_ptr, table_path_len).map_err(|e| format!("df_create_reader: {}", e))?;
    let mut filenames = Vec::with_capacity(files_count as usize);
    for i in 0..files_count as usize {
        let ptr = *files_ptr.add(i);
        let len = *files_len_ptr.add(i);
        filenames.push(str_from_raw(ptr, len).map_err(|e| format!("df_create_reader: {}", e))?.to_string());
    }
    let mgr = get_rt_manager()?;
    api::create_reader(table_path, filenames, &mgr).map_err(|e| e.to_string())
}

#[no_mangle]
pub unsafe extern "C" fn df_close_reader(ptr: i64) {
    api::close_reader(ptr);
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_execute_query(
    shard_view_ptr: i64,
    table_name_ptr: *const u8,
    table_name_len: i64,
    plan_ptr: *const u8,
    plan_len: i64,
    runtime_ptr: i64,
    context_id: i64,
) -> i64 {
    let mgr = get_rt_manager()?;
    let table_name = str_from_raw(table_name_ptr, table_name_len)
        .map_err(|e| format!("df_execute_query: {}", e))?;
    let plan_bytes = slice::from_raw_parts(plan_ptr, plan_len as usize);

    match get_execution_mode() {
        ExecutionMode::BlockOn | ExecutionMode::Hybrid => {
            // BlockOn: current behavior — block on io_runtime.
            // Hybrid: same path — CrossRtStream already dispatches CPU work
            // to cpu_executor internally via DedicatedExecutor.
            mgr.io_runtime
                .block_on(api::execute_query(
                    shard_view_ptr, table_name, plan_bytes,
                    runtime_ptr, &mgr, context_id,
                ))
                .map_err(|e| e.to_string())
        }
        ExecutionMode::Spawn => {
            // Copy data that needs to cross thread boundary
            let table_name_owned = table_name.to_string();
            let plan_owned = plan_bytes.to_vec();
            let mgr_clone = mgr.clone();
            let svp = SendPtr(shard_view_ptr);
            let rtp = SendPtr(runtime_ptr);
            spawn_and_wait_io(&mgr.io_runtime, async move {
                api::execute_query(
                    svp.0, &table_name_owned, &plan_owned,
                    rtp.0, &mgr_clone, context_id,
                ).await
            })
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn df_execute_query_async(
    shard_view_ptr: i64,
    table_name_ptr: *const u8,
    table_name_len: i64,
    plan_ptr: *const u8,
    plan_len: i64,
    runtime_ptr: i64,
    context_id: i64,
    listener_id: i64,
) {
    // Get callback — if not registered, log and return (no listener to notify)
    let cb = match get_async_callback() {
        Ok(cb) => cb,
        Err(e) => {
            log::error!("df_execute_query_async: {}", e);
            return;
        }
    };
    // Get runtime manager — if not initialized, notify listener with error
    let mgr = match get_rt_manager() {
        Ok(m) => m,
        Err(e) => {
            let err_ptr = into_error_ptr(e);
            (cb.callback)(listener_id, err_ptr, 1);
            return;
        }
    };

    // Copy string/byte args to owned types BEFORE returning to Java.
    // The Java NativeCall arena closes immediately after this function returns,
    // so we must not hold references to arena-allocated memory.
    let table_name_owned = match str_from_raw(table_name_ptr, table_name_len) {
        Ok(s) => s.to_string(),
        Err(e) => {
            let err_ptr = into_error_ptr(format!("df_execute_query_async: {}", e));
            (cb.callback)(listener_id, err_ptr, 1);
            return;
        }
    };
    let plan_owned = std::slice::from_raw_parts(plan_ptr, plan_len as usize).to_vec();

    let mgr_clone = mgr.clone();
    let svp = SendPtr(shard_view_ptr);
    let rtp = SendPtr(runtime_ptr);
    let callback_fn = cb.callback;

    mgr.io_runtime.spawn(async move {
        match api::execute_query(
            svp.0, &table_name_owned, &plan_owned,
            rtp.0, &mgr_clone, context_id,
        ).await {
            Ok(ptr) => (callback_fn)(listener_id, ptr, 0),
            Err(e) => {
                let err_ptr = into_error_ptr(e.to_string());
                (callback_fn)(listener_id, err_ptr, 1);
            }
        }
    });
    // Returns immediately — Java thread is free
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_stream_get_schema(stream_ptr: i64) -> i64 {
    api::stream_get_schema(stream_ptr).map_err(|e| e.to_string())
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_stream_next(stream_ptr: i64) -> i64 {
    let mgr = get_rt_manager()?;

    match get_execution_mode() {
        ExecutionMode::BlockOn => {
            // Current behavior — block on io_runtime
            mgr.io_runtime
                .block_on(api::stream_next(stream_ptr))
                .map_err(|e| e.to_string())
        }
        ExecutionMode::Spawn => {
            // Spawn on io_runtime, block FFM thread on channel
            let ptr = SendPtr(stream_ptr);
            spawn_and_wait_io(&mgr.io_runtime, async move {
                api::stream_next(ptr.0).await
            })
        }
        ExecutionMode::Hybrid => {
            // Stream polling is CPU-bound — dispatch to cpu_executor
            let ptr = SendPtr(stream_ptr);
            let cpu = mgr.cpu_executor();
            spawn_and_wait_cpu(&cpu, async move {
                api::stream_next(ptr.0).await
            })
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn df_stream_next_async(
    stream_ptr: i64,
    listener_id: i64,
) {
    let cb = match get_async_callback() {
        Ok(cb) => cb,
        Err(e) => { log::error!("df_stream_next_async: {}", e); return; }
    };
    let mgr = match get_rt_manager() {
        Ok(m) => m,
        Err(e) => {
            let err_ptr = into_error_ptr(e);
            (cb.callback)(listener_id, err_ptr, 1);
            return;
        }
    };

    let ptr = SendPtr(stream_ptr);
    let callback_fn = cb.callback;

    mgr.io_runtime.spawn(async move {
        match api::stream_next(ptr.0).await {
            Ok(batch_ptr) => (callback_fn)(listener_id, batch_ptr, 0),
            Err(e) => {
                let err_ptr = into_error_ptr(e.to_string());
                (callback_fn)(listener_id, err_ptr, 1);
            }
        }
    });
}

#[no_mangle]
pub unsafe extern "C" fn df_stream_close(stream_ptr: i64) {
    api::stream_close(stream_ptr);
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_sql_to_substrait(
    shard_view_ptr: i64,
    table_name_ptr: *const u8,
    table_name_len: i64,
    sql_ptr: *const u8,
    sql_len: i64,
    runtime_ptr: i64,
    out_ptr: *mut u8,
    out_cap: i64,
    out_len: *mut i64,
) -> i64 {
    let mgr = get_rt_manager()?;
    let table_name = str_from_raw(table_name_ptr, table_name_len).map_err(|e| format!("df_sql_to_substrait: table_name: {}", e))?;
    let sql = str_from_raw(sql_ptr, sql_len).map_err(|e| format!("df_sql_to_substrait: sql: {}", e))?;
    let bytes = api::sql_to_substrait(shard_view_ptr, table_name, sql, runtime_ptr, &mgr)
        .map_err(|e| e.to_string())?;
    if bytes.len() > out_cap as usize {
        return Err(format!(
            "substrait plan size {} exceeds buffer capacity {}",
            bytes.len(),
            out_cap
        ));
    }
    std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_ptr, bytes.len());
    if !out_len.is_null() {
        *out_len = bytes.len() as i64;
    }
    Ok(0)
}
