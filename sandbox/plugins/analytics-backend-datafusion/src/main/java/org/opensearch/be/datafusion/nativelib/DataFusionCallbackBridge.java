/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.action.ActionListener;
import org.opensearch.nativebridge.spi.NativeLibraryLoader;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages the listener registry and upcall stub for async FFM callbacks from Rust.
 *
 * <p>When Rust completes an async operation (query execution, stream next), it invokes
 * the upcall stub with {@code (listener_id, result, is_error)}. This class looks up
 * the corresponding {@link ActionListener} and dispatches the result or error.</p>
 *
 * <p>The upcall stub is created once during initialization with {@link Arena#global()},
 * so it lives for the JVM process lifetime. No JVM thread attachment is needed —
 * FFM upcalls are safe to call from any native thread.</p>
 *
 * @see org.opensearch.nativebridge.spi.RustLoggerBridge for the upcall pattern reference
 */
public final class DataFusionCallbackBridge {

    private static final Logger logger = LogManager.getLogger(DataFusionCallbackBridge.class);

    /** Pending async listeners keyed by listener ID. */
    private static final ConcurrentHashMap<Long, ActionListener<Long>> PENDING = new ConcurrentHashMap<>();

    /** Monotonic ID generator for listener IDs. */
    private static final AtomicLong ID_GEN = new AtomicLong();

    /** The upcall stub function pointer, created once. */
    private static volatile MemorySegment upcallStub;

    private DataFusionCallbackBridge() {}

    /**
     * Registers a listener and returns its unique ID.
     * The listener will be invoked exactly once when the async operation completes.
     */
    public static long registerListener(ActionListener<Long> listener) {
        long id = ID_GEN.incrementAndGet();
        PENDING.put(id, listener);
        return id;
    }

    /**
     * Static callback invoked by Rust via the upcall stub.
     * Signature must match: void(long listenerId, long result, int isError)
     */
    static void onAsyncComplete(long listenerId, long result, int isError) {
        ActionListener<Long> listener = PENDING.remove(listenerId);
        if (listener == null) {
            logger.warn("Async callback for unknown listener ID {}", listenerId);
            return;
        }
        if (isError == 0) {
            listener.onResponse(result);
        } else {
            // result is an error pointer — read and free it
            String errorMsg = readAndFreeNativeError(result);
            listener.onFailure(new RuntimeException(errorMsg));
        }
    }

    /**
     * Creates the upcall stub and registers it with Rust.
     * Called once during DataFusionService.doStart().
     *
     * @param registerHandle MethodHandle for df_register_async_callback
     */
    public static void initialize(MethodHandle registerHandle) {
        try {
            MethodHandle callbackHandle = MethodHandles.lookup().findStatic(
                DataFusionCallbackBridge.class,
                "onAsyncComplete",
                MethodType.methodType(void.class, long.class, long.class, int.class)
            );
            upcallStub = Linker.nativeLinker().upcallStub(
                callbackHandle,
                FunctionDescriptor.ofVoid(
                    ValueLayout.JAVA_LONG,  // listener_id
                    ValueLayout.JAVA_LONG,  // result
                    ValueLayout.JAVA_INT    // is_error
                ),
                Arena.global()
            );
            // Pass the function pointer to Rust
            registerHandle.invokeExact(upcallStub);
            logger.info("Async callback bridge initialized");
        } catch (Throwable t) {
            logger.error("Failed to initialize async callback bridge", t);
            throw new RuntimeException("Async callback registration failed", t);
        }
    }

    /**
     * Reads and frees a native error string using the standard error convention.
     * Delegates to NativeLibraryLoader's error handling.
     */
    private static String readAndFreeNativeError(long errPtr) {
        // Use checkResult which reads and frees the error, throwing RuntimeException
        try {
            NativeLibraryLoader.checkResult(-errPtr); // negate to trigger error path
            return "unknown error"; // unreachable
        } catch (RuntimeException e) {
            return e.getMessage();
        }
    }
}
