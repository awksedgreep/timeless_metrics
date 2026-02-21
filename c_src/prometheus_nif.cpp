// Prometheus text format parser NIF for TimelessMetrics
//
// Parses the entire Prometheus exposition body in one NIF call,
// returning {entries_list, error_count} to Elixir.
//
// Each entry: {metric_name_binary, labels_proplist, value_double, timestamp_int64}
// Lines without timestamps use 0 as sentinel.

#include <erl_nif.h>
#include <cstring>
#include <cstdlib>
#include <cmath>

// Parse a double from a string range. Returns true on success.
// Rejects Inf/NaN since BEAM doesn't support them as float values.
static bool parse_double(const char* start, const char* end, double* out) {
    size_t len = end - start;
    if (len == 0) return false;

    // Reject special Prometheus values (BEAM can't represent them)
    if (len >= 3) {
        if (memcmp(start, "NaN", 3) == 0) return false;
        if (memcmp(start, "Inf", 3) == 0) return false;
        if (len >= 4 && (memcmp(start, "+Inf", 4) == 0 || memcmp(start, "-Inf", 4) == 0))
            return false;
    }

    // Stack buffer for null-termination
    char buf[64];
    if (len >= sizeof(buf)) return false;
    memcpy(buf, start, len);
    buf[len] = '\0';

    char* endp;
    *out = strtod(buf, &endp);
    if (endp == buf || *endp != '\0') return false;

    // Double-check result isn't inf/nan (e.g., overflow)
    if (std::isinf(*out) || std::isnan(*out)) return false;

    return true;
}

// Parse a 64-bit integer from a string range. Returns true on success.
static bool parse_int64(const char* start, const char* end, int64_t* out) {
    char buf[32];
    size_t len = end - start;
    if (len == 0 || len >= sizeof(buf)) return false;
    memcpy(buf, start, len);
    buf[len] = '\0';

    char* endp;
    *out = strtoll(buf, &endp, 10);
    return endp != buf && *endp == '\0';
}

// Skip whitespace, returns pointer to first non-space char
static const char* skip_ws(const char* p, const char* end) {
    while (p < end && (*p == ' ' || *p == '\t')) p++;
    return p;
}

// Trim trailing whitespace, returns pointer past last non-space char
static const char* rtrim(const char* start, const char* end) {
    while (end > start && (*(end-1) == ' ' || *(end-1) == '\t' || *(end-1) == '\r')) end--;
    return end;
}

// Make an Erlang binary from a char range
static ERL_NIF_TERM make_binary(ErlNifEnv* env, const char* start, size_t len) {
    ERL_NIF_TERM bin;
    unsigned char* buf = enif_make_new_binary(env, len, &bin);
    memcpy(buf, start, len);
    return bin;
}

// Parse labels from "{key=\"val\",key2=\"val2\"}" format
// Returns a list of {key_binary, value_binary} tuples
static ERL_NIF_TERM parse_labels(ErlNifEnv* env, const char* start, const char* end) {
    ERL_NIF_TERM list = enif_make_list(env, 0);

    if (start >= end) return list;

    // Build list in reverse, then return (proplist order doesn't matter for maps)
    const char* p = start;
    while (p < end) {
        // Skip whitespace and commas
        while (p < end && (*p == ' ' || *p == ',')) p++;
        if (p >= end) break;

        // Find '='
        const char* eq = (const char*)memchr(p, '=', end - p);
        if (!eq) break;

        const char* key_start = p;
        const char* key_end = eq;

        // Trim trailing whitespace on key
        while (key_end > key_start && *(key_end-1) == ' ') key_end--;

        p = eq + 1;

        // Expect '"' after '='
        if (p >= end || *p != '"') break;
        p++; // skip opening quote

        // Find closing quote (handle escaped quotes)
        const char* val_start = p;
        while (p < end && *p != '"') {
            if (*p == '\\' && p + 1 < end) {
                p += 2; // skip escaped char
            } else {
                p++;
            }
        }
        const char* val_end = p;
        if (p < end) p++; // skip closing quote

        ERL_NIF_TERM key_bin = make_binary(env, key_start, key_end - key_start);
        ERL_NIF_TERM val_bin = make_binary(env, val_start, val_end - val_start);
        ERL_NIF_TERM pair = enif_make_tuple2(env, key_bin, val_bin);
        list = enif_make_list_cell(env, pair, list);
    }

    return list;
}

// Parse a single Prometheus line.
// Returns true on success and fills out the entry term.
static bool parse_line(ErlNifEnv* env, const char* line_start, const char* line_end,
                       ERL_NIF_TERM* entry) {
    const char* p = skip_ws(line_start, line_end);
    line_end = rtrim(p, line_end);
    if (p >= line_end) return false;

    // Skip comments and type/help lines
    if (*p == '#') return false;

    // Find metric name end (either '{' for labels or ' ' for no labels)
    const char* metric_end = p;
    while (metric_end < line_end && *metric_end != '{' && *metric_end != ' ' && *metric_end != '\t')
        metric_end++;

    if (metric_end == p || metric_end >= line_end) return false;

    ERL_NIF_TERM metric_bin = make_binary(env, p, metric_end - p);
    ERL_NIF_TERM labels;
    const char* after_labels;

    if (*metric_end == '{') {
        // Has labels: find closing '}'
        const char* brace_close = (const char*)memchr(metric_end + 1, '}', line_end - metric_end - 1);
        if (!brace_close) return false;

        labels = parse_labels(env, metric_end + 1, brace_close);
        after_labels = brace_close + 1;
    } else {
        // No labels
        labels = enif_make_list(env, 0);
        after_labels = metric_end;
    }

    // Skip whitespace after metric/labels
    after_labels = skip_ws(after_labels, line_end);
    if (after_labels >= line_end) return false;

    // Parse value
    const char* value_start = after_labels;
    const char* value_end = after_labels;
    while (value_end < line_end && *value_end != ' ' && *value_end != '\t') value_end++;

    double value;
    if (!parse_double(value_start, value_end, &value)) return false;

    // Parse optional timestamp
    int64_t timestamp = 0;
    const char* ts_start = skip_ws(value_end, line_end);
    if (ts_start < line_end) {
        const char* ts_end = ts_start;
        while (ts_end < line_end && *ts_end != ' ' && *ts_end != '\t') ts_end++;
        if (!parse_int64(ts_start, ts_end, &timestamp)) {
            // Timestamp parse failure is not fatal; use 0 sentinel
            timestamp = 0;
        }
    }

    *entry = enif_make_tuple4(env, metric_bin, labels,
                               enif_make_double(env, value),
                               enif_make_int64(env, timestamp));
    return true;
}

static ERL_NIF_TERM nif_parse(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    ErlNifBinary body;
    if (!enif_inspect_binary(env, argv[0], &body)) {
        return enif_make_badarg(env);
    }

    const char* data = (const char*)body.data;
    size_t size = body.size;
    const char* end = data + size;

    ERL_NIF_TERM list = enif_make_list(env, 0);
    int error_count = 0;
    int entry_count = 0;

    const char* line_start = data;
    while (line_start < end) {
        // Find end of line
        const char* line_end = (const char*)memchr(line_start, '\n', end - line_start);
        if (!line_end) line_end = end;

        ERL_NIF_TERM entry;
        if (parse_line(env, line_start, line_end, &entry)) {
            list = enif_make_list_cell(env, entry, list);
            entry_count++;
        } else {
            // Check if it was a non-empty, non-comment line (actual parse error)
            const char* p = skip_ws(line_start, line_end);
            const char* trimmed_end = rtrim(p, line_end);
            if (p < trimmed_end && *p != '#') {
                error_count++;
            }
        }

        line_start = line_end + 1;
    }

    // Reverse the list to preserve line order
    ERL_NIF_TERM reversed;
    if (!enif_make_reverse_list(env, list, &reversed)) {
        reversed = list;
    }

    return enif_make_tuple2(env, reversed, enif_make_int(env, error_count));
}

static ErlNifFunc nif_funcs[] = {
    {"parse", 1, nif_parse, 0}
};

ERL_NIF_INIT(Elixir.TimelessMetrics.PrometheusNif, nif_funcs, NULL, NULL, NULL, NULL)
