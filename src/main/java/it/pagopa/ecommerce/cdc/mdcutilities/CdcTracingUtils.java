package it.pagopa.ecommerce.cdc.mdcutilities;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.ecommerce.commons.documents.v2.TransactionEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import reactor.util.context.Context;

/**
 * Utility class with helper methods to enrich Reactor Context for CDC event
 * processing.
 */
public class CdcTracingUtils {

    private static final String CTX_DETAILS_KEY = "ctx.details";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Logger LOGGER = LoggerFactory.getLogger(CdcTracingUtils.class);

    private CdcTracingUtils() {
    }

    /**
     * Tracing keys used in MDC and/or propagated from Reactor Context.
     *
     * <p>
     * Entries marked as {@code contextBound = true} are copied from Reactor Context
     * to MDC by {@link MDCContextLifter}. Entries marked as {@code false} are
     * written locally in MDC (for example by {@link #withErrorMdc}).
     */
    public enum TracingEntry {
        CTX_TRANSACTION_ID("ctx.transaction.id", "{transactionId-not-found}", true),
        CTX_EVENT_CODE("ctx.event.code", "{eventCode-not-found}", true),
        CTX_EVENT_ID("ctx.event.id", "{eventId-not-found}", true),
        EVENT_ACTION("event.action", "{eventAction-not-found}", true),
        DEPENDENCY("dependency", "{dependency-not-found}", false),
        ERROR_TYPE("error.type", "{errorType-not-found}", false),
        ERROR_MESSAGE("error.message", "{errorMessage-not-found}", false);

        private final String key;
        private final String defaultValue;
        private final boolean contextBound;

        TracingEntry(
                String key,
                String defaultValue,
                boolean contextBound
        ) {
            this.key = key;
            this.defaultValue = defaultValue;
            this.contextBound = contextBound;
        }

        public String getKey() {
            return key;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public boolean isContextBound() {
            return contextBound;
        }
    }

    /** Enrich Reactor Context with CDC event metadata used by MDC/logging hooks. */
    public static Context enrichContextForCdcEvent(
                                                   TransactionEvent<?> event,
                                                   Context reactorContext
    ) {
        return reactorContext
                .put(
                        TracingEntry.CTX_TRANSACTION_ID.getKey(),
                        event.getTransactionId() != null
                                ? event.getTransactionId()
                                : TracingEntry.CTX_TRANSACTION_ID.getDefaultValue()
                )
                .put(
                        TracingEntry.CTX_EVENT_CODE.getKey(),
                        event.getEventCode() != null
                                ? event.getEventCode()
                                : TracingEntry.CTX_EVENT_CODE.getDefaultValue()
                )
                .put(
                        TracingEntry.CTX_EVENT_ID.getKey(),
                        event.getId() != null
                                ? event.getId()
                                : TracingEntry.CTX_EVENT_ID.getDefaultValue()
                )
                .put(TracingEntry.EVENT_ACTION.getKey(), "PROCESS_CDC_EVENT");
    }

    /**
     * Executes a block with structured error details temporarily inserted in MDC.
     *
     * <p>
     * The method adds {@code error.type} and {@code error.message} keys, executes
     * the provided block, and always removes those keys afterward.
     *
     * @param error error instance used to populate MDC metadata
     * @param block code to execute while error metadata is available in MDC
     */
    public static void withErrorMdc(
                                    Throwable error,
                                    Runnable block
    ) {
        insertIntoMdcAndCleanup(
                Map.of(
                        TracingEntry.ERROR_TYPE.getKey(),
                        error != null
                                ? error.getClass().getName()
                                : TracingEntry.ERROR_TYPE.getDefaultValue(),
                        TracingEntry.ERROR_MESSAGE.getKey(),
                        error != null && error.getMessage() != null
                                ? error.getMessage()
                                : TracingEntry.ERROR_MESSAGE.getDefaultValue()
                ),
                block
        );
    }

    /**
     * Executes a block with {@code ctx.details} temporarily stored in MDC as a JSON
     * string.
     *
     * <p>
     * The input map is serialized to raw JSON and stored under key
     * {@code ctx.details}. If serialization fails, an empty JSON object
     * ({@code {}}) is used as fallback. The key is always removed after block
     * execution.
     *
     * @param details map of detail values to serialize under {@code ctx.details}
     * @param block   code to execute while {@code ctx.details} is available in MDC
     */
    public static void withContextDetailsMdc(
                                             Map<String, ?> details,
                                             Runnable block
    ) {
        String rawDetails = "{}";
        if (details != null) {
            try {
                rawDetails = OBJECT_MAPPER.writeValueAsString(details);
            } catch (JsonProcessingException ignored) {
                rawDetails = "{}";
            }
        }

        insertIntoMdcAndCleanup(
                Map.of(CTX_DETAILS_KEY, rawDetails),
                block
        );
    }

    /**
     * Inserts the provided entries into MDC, executes the given block, and always
     * removes the inserted keys afterward.
     *
     * <p>
     * This method guarantees MDC cleanup through a {@code finally} block, so
     * temporary values do not leak across log statements or threads.
     *
     * @param entries key/value pairs to temporarily add to MDC
     * @param block   code to execute while MDC entries are available
     */
    private static void insertIntoMdcAndCleanup(
                                                Map<String, ?> entries,
                                                Runnable block
    ) {
        List<String> detailKeys = new ArrayList<>();

        try {
            if (entries != null) {
                entries.forEach(
                        (
                         key,
                         value
                        ) -> {
                            if (key != null && value != null) {
                                MDC.put(key, value.toString());
                                detailKeys.add(key);
                            }
                        }
                );
            }
            block.run();
        } finally {
            detailKeys.forEach(MDC::remove);
        }
    }
}
